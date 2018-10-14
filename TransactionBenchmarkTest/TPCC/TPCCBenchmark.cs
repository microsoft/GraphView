using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;
using GraphView.Transaction;
using System.Threading;

namespace TransactionBenchmarkTest.TPCC
{
    class TPCCWorker
    {
        private WorkloadParam[] workloads;

        private int workloadCount = 0;

        //private int workloadCount;
        public int commitCount;
        public int abortCount;

        private SyncExecution execution;

        private volatile bool isFinished;

        public TPCCWorker(SyncExecution execution, int workloadCount = 0)
        {
            this.commitCount = this.abortCount = 0;
            this.execution = execution;
            this.workloads = new WorkloadParam[0];
            this.workloadCount = workloadCount;
        }
        public void SetWorkload(WorkloadParam[] workloads)
        {
            this.workloads = workloads;
        }

        public bool IsFinished
        {
            get { return isFinished; }
            set { isFinished = value; }
        }

        public void Run()
        {
            for (int i = 0; i < this.workloadCount; ++i)
            {
                WorkloadParam workload =
                    this.workloads[i % this.workloads.Length];
                var ret = workload.Execute(this.execution);
                if (ret.txFinalStatus == TxFinalStatus.COMMITTED)
                    this.commitCount++;
                else if (ret.txFinalStatus == TxFinalStatus.ABORTED)
                    this.abortCount++;
            }
            isFinished = true;
        }
    }

    class WorkloadLoader
    {
        class CircularLoader<T>
        {
            public CircularLoader(IEnumerable<T> data)
            {
                this.data = data.ToArray();
                this.index = 0;
            }
            public IEnumerable<T> GetChunk(int length)
            {
                for (int i = 0; i < length; ++i)
                {
                    yield return data[this.index];
                    this.index = (this.index + 1) % this.data.Length;
                }
            }
            T[] data;
            int index;
        }
        public WorkloadLoader(string workloadDir)
        {
            this.workloadDir = workloadDir;
        }
        public IEnumerable<string[]> NextColumns(int n, string workloadName) {
            switch (workloadName)
            {
                case "PAYMENT": return NextPaymentColumns(n);
                case "NEW_ORDER": return NextNewOrderColumns(n);
            }
            throw new Exception($"unknown workload {workloadName}");
        }
        public IEnumerable<string[]> NextPaymentColumns(int n)
        {
            return this.PaymentLoader().GetChunk(n);
        }
        public IEnumerable<string[]> NextNewOrderColumns(int n)
        {
            return this.NewOrderLoader().GetChunk(n);
        }
        private CircularLoader<string[]> PaymentLoader()
        {
            return GetOrCreate(ref this.paymentLoader, "PAYMENT");
        }
        private CircularLoader<string[]> NewOrderLoader()
        {
            return GetOrCreate(ref this.newOrderLoader, "NEW_ORDER");
        }
        private CircularLoader<string[]> GetOrCreate(
            ref CircularLoader<string[]> loader, string name)
        {
            if (loader == null)
            {
                loader = new CircularLoader<string[]>(
                    FileHelper.LoadCsv($"{this.workloadDir}\\{name}.csv", true));
            }
            return loader;
        }

        private string workloadDir;
        private CircularLoader<string[]> paymentLoader;
        private CircularLoader<string[]> newOrderLoader;
    }

    abstract class WorkloadAllocator
    {
        public WorkloadAllocator(WorkloadLoader loader)
        {
            this.paymentBuilder = new PaymentWorkloadBuilder();
            this.newOrderBuilder = new NewOrderWorkloadBuilder();
            this.loader = loader;
        }
        public WorkloadParam[] Allocate(int n, int workerId, int totalWorker)
        {
            this.paymentBuilder.ResetStoredProcedure();
            this.newOrderBuilder.ResetStoredProcedure();
            return this.AllocateImpl(n, workerId, totalWorker);
        }
        static private IEnumerable<WorkloadParam> GetParams(
            int n, WorkloadLoader loader, WorkloadBuilder builder) {
            if (n == 0)
            {
                return Enumerable.Empty<WorkloadParam>();
            }
            builder.NewStoredProcedureIfNon();
            return loader.NextColumns(n, builder.Name())
                .Select(builder.BuildWorkload);
        }

        protected IEnumerable<WorkloadParam> GetPayments(int n)
        {
            return GetParams(n, this.loader, this.paymentBuilder);
        }
        protected IEnumerable<WorkloadParam> GetNewOrders(int n)
        {
            return GetParams(n, this.loader, this.newOrderBuilder);
        }

        protected abstract
        WorkloadParam[] AllocateImpl(int n, int workerId, int totalWorker);

        private WorkloadLoader loader;
        private WorkloadBuilder paymentBuilder;
        private WorkloadBuilder newOrderBuilder;
    }

    class HybridAllocator : WorkloadAllocator
    {
        public HybridAllocator(WorkloadLoader loader, double paymentRatio) : base(loader)
        {
            this.paymentRatio = paymentRatio;
        }
        protected override
        WorkloadParam[] AllocateImpl(int n, int workerId, int totalWorker)
        {
            int paymentNum = (int)(n * this.paymentRatio);
            int newOrderNum = n - paymentNum;
            Random random = new Random();
            WorkloadParam[] workloads = GetPayments(paymentNum)
                .Concat(GetNewOrders(newOrderNum))
                .OrderBy(_ => random.Next())
                .ToArray();
            return workloads;
        }
        private double paymentRatio;
    }

    class TPCCBenchmark
    {
        private int workerCount;
        private int workloadCountPerWorker;

        private TPCCWorker[] tpccWorkers;

        private DateTime startTicks;
        private DateTime endTicks;

        static TPCCWorker[] InititializeWorkers(
            SyncExecutionBuilder builder, int workloadCount)
        {
            SyncExecution[] execs = builder.BuildAll();
            var workers = new TPCCWorker[execs.Length];
            for (int i = 0; i < workers.Length; ++i)
            {
                workers[i] = new TPCCWorker(execs[i], workloadCount);
            }
            return workers;
        }

        public TPCCBenchmark(
            SyncExecutionBuilder builder, int workloadCountPerWorker)
        {
            this.workloadCountPerWorker = workloadCountPerWorker;

            this.tpccWorkers = InititializeWorkers(
                builder, workloadCountPerWorker);
            this.workerCount = this.tpccWorkers.Length;
        }

        static private int CalculateWorkload(int total, int workers)
        {
            if (workers == 0)
            {
                return 0;
            }
            return (total + workers - 1) / workers;
        }

        public void AllocateWorkload(WorkloadAllocator allocator)
        {
            Console.Write("Start Loading workload... ");
            DateTime start = DateTime.UtcNow;
            for (int i = 0; i < this.tpccWorkers.Length; ++i) {
                WorkloadParam[] workloads = allocator.Allocate(
                    this.workloadCountPerWorker, i, this.workerCount);
                this.tpccWorkers[i].SetWorkload(workloads);
            }
            DateTime end = DateTime.UtcNow;
            Console.WriteLine($"Done ({(end - start).TotalSeconds:F3} sec)");
        }

        static double TotalMemoryInMB()
        {
            return GC.GetTotalMemory(false) / 1024.0 / 1024.0;
        }

        static void ForceGC()
        {
            Console.WriteLine($"Before GC: {TotalMemoryInMB():F3}MB is used");
            GC.Collect();
            Console.WriteLine($"After GC: {TotalMemoryInMB():F3}MB is used");
        }

        private void PrintMonitorInfo(double time, int threadsAlive, int deltaAbort, int deltaCommit)
        {
            int sum = deltaCommit + deltaAbort;
            if (sum == 0) return;
            Console.WriteLine($"Time:{time:F3}|Count:{sum}|Throughput:{sum / time:F2}|AbortRate:{deltaAbort * 1.0 / sum:F3}|ThreadsAlive:{threadsAlive}");
        }

        private void MonitorThroughput(int ms)
        {
            long lastTime = DateTime.Now.Ticks;
            int lastCommit = 0;
            int lastAbort = 0;

            int threadsAlive = this.workerCount;
            do
            {
                threadsAlive = this.workerCount;
                Thread.Sleep(ms);
                long currentTime = DateTime.Now.Ticks;
                int nowCommit = 0;
                int nowAbort = 0;
                for (int i = 0; i < workerCount; i++)
                {
                    nowCommit += tpccWorkers[i].commitCount;
                    nowAbort += tpccWorkers[i].abortCount;
                    if (tpccWorkers[i].IsFinished)
                    {
                        --threadsAlive;
                    }
                }
                double time = (currentTime - lastTime) / 10000000.0;
                PrintMonitorInfo(time, threadsAlive, nowAbort - lastAbort, nowCommit - lastCommit);

                lastTime = currentTime;
                lastCommit = nowCommit;
                lastAbort = nowAbort;
            } while (threadsAlive != 0);
        }

        public void Run()
        {
            Console.WriteLine("Running TPCC workload...");
            ForceGC();
            this.startTicks = DateTime.UtcNow;

            Thread[] threads = new Thread[workerCount];
            for (int i = 0; i < this.workerCount; i++)
            {
                threads[i] = new Thread(new ThreadStart(tpccWorkers[i].Run));
                threads[i].Start();
            }

            MonitorThroughput(100);

            foreach (Thread thread in threads)
            {
                thread.Join();
            }

            this.endTicks = DateTime.UtcNow;
        }

        public void PrintStats()
        {
            int committed = this.tpccWorkers.Select(worker => worker.commitCount).Sum();
            int aborted = this.tpccWorkers.Select(worker => worker.abortCount).Sum();
            Console.WriteLine($"Committed: {committed}, aborted: {aborted}");
            Console.WriteLine($"Abort rate: {(double)aborted / (committed + aborted):F3}");
        }

        internal int Throughput
        {
            get
            {
                double seconds = (this.endTicks - this.startTicks).TotalSeconds;
                int workloadTotal = this.workerCount * this.workloadCountPerWorker;
                Console.WriteLine("Processed {0} workloads in {1} seconds", workloadTotal, seconds);
                return (int)(workloadTotal / seconds);
            }
        }

    }
}
