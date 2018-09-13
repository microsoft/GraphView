using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;
using GraphView.Transaction;
using System.Threading;
using Newtonsoft.Json;

namespace TransactionBenchmarkTest.TPCC
{
    class TPCCWorker
    {
        private Queue<TPCCWorkload> tpccWorkloadQueue;

        //private int workloadCount;
        public int commitCount;
        public int abortCount;

        private TransactionExecution execution;

        public TPCCWorker(TransactionExecution execution)
        {
            this.commitCount = this.abortCount = 0;
            this.execution = execution;
            this.tpccWorkloadQueue = new Queue<TPCCWorkload>();
        }

        public void Run()
        {
            foreach (var workload in tpccWorkloadQueue)
            {
                var ret = workload.Run(execution);
                if (ret.txFinalStatus == TxFinalStatus.COMMITTED)
                    this.commitCount++;
                else if (ret.txFinalStatus == TxFinalStatus.ABORTED)
                    this.abortCount++;
            }
        }

        public void AddWorkload(TPCCWorkload tpccWorkload)
        {
            this.tpccWorkloadQueue.Enqueue(tpccWorkload);
        }

    }

    class TPCCBenchmark
    {
        private int workerCount;
        private int workloadCountPerWorker;

        private TPCCWorker[] tpccWorkers;

        private long startTicks;
        private long endTicks;

        static private
        TransactionExecution[] InitializeExecEnvs(VersionDb versionDb)
        {
            int workerCount = versionDb.PartitionCount;
            TransactionExecution[] execs = new TransactionExecution[workerCount];
            for (int i = 0; i < workerCount; ++i)
            {
                execs[i] = new TransactionExecution(
                    null, versionDb, null, new TxRange(i), i);
            }
            return execs;
        }

        static TPCCWorker[] InititializeWorkers(VersionDb versionDb)
        {
            int workerCount = versionDb.PartitionCount;
            var workers = new TPCCWorker[workerCount];
            var execs = InitializeExecEnvs(versionDb);
            for (int i = 0; i < workerCount; ++i)
            {
                workers[i] = new TPCCWorker(execs[i]);
            }
            return workers;
        }

        public TPCCBenchmark(VersionDb versionDb, int workloadCountPerWorker)
        {
            this.workerCount = versionDb.PartitionCount;
            this.workloadCountPerWorker = workloadCountPerWorker;

            this.tpccWorkers = InititializeWorkers(versionDb);
        }

        public void LoadNewOrderWorkload(string filepath)
        {
            Console.WriteLine("Loading New-Order workload");
            var csvReader = new System.IO.StreamReader(filepath);
            string line = null;
            int lineNum = 0;
            line = csvReader.ReadLine();    // ignore the first line

            int workloadTotal = workerCount * workloadCountPerWorker;

            while ((line = csvReader.ReadLine()) != null)       // if not enough?
            {
                string[] columns = line.Split(new string[] { Constants.WorkloadDelimiter }, StringSplitOptions.None);
                for (int j = 0; j < columns.Length; j++) { columns[j] = columns[j].Substring(1); }
                columns[columns.Length - 1] = columns[columns.Length - 1].Substring(0, columns[columns.Length - 1].Length - 1); // remove `"`

                var no = new NewOrderInParameters
                {
                    timestamp = columns[0],
                    W_ID = Convert.ToUInt32(columns[5]),
                    D_ID = Convert.ToUInt32(columns[3]),
                    C_ID = Convert.ToUInt32(columns[1]),
                    OL_I_IDs = JsonConvert.DeserializeObject<uint[]>(columns[6]),
                    OL_SUPPLY_W_IDs = JsonConvert.DeserializeObject<uint[]>(columns[4]),
                    OL_QUANTITYs = JsonConvert.DeserializeObject<uint[]>(columns[2]),
                    O_ENTRY_D = columns[7]
                };

                int i = lineNum++ / workloadCountPerWorker;

                TPCCNewOrderWorkload neworder = new TPCCNewOrderWorkload(no/*, this.tpccWorkers[i].vdb, this.tpccWorkers[i].redisClient*/);
                this.tpccWorkers[i].AddWorkload(neworder);

                if (lineNum == workloadTotal) break;
            }
            if (lineNum != workloadTotal) throw new Exception("there is no enough workload");
        }

        public void LoadPaymentWorkload(string filepath)
        {
            Console.WriteLine("Loading PAYMENT workload...");
            var csvReader = new System.IO.StreamReader(filepath);
            string line = null;
            int lineNum = 0;
            line = csvReader.ReadLine();    // ignore the first line: header

            int workloadTotal = workerCount * workloadCountPerWorker;

            while ((line = csvReader.ReadLine()) != null)
            {
                string[] columns = line.Split(new string[] { Constants.WorkloadDelimiter }, StringSplitOptions.None);
                for (int j = 0; j < columns.Length; j++) { columns[j] = columns[j].Substring(1); }
                columns[columns.Length - 1] = columns[columns.Length - 1].Substring(0, columns[columns.Length - 1].Length - 1); // remove `"`

                var pm = new PaymentInParameters
                {
                    timestamp = columns[0],
                    C_ID = (columns[1] == "" ? 0 : Convert.ToUInt32(columns[1])),
                    C_LAST = columns[2],    // may be ""
                    H_DATE = columns[3],
                    C_D_ID = Convert.ToUInt32(columns[4]),
                    D_ID = Convert.ToUInt32(columns[5]),
                    W_ID = Convert.ToUInt32(columns[6]),
                    C_W_ID = Convert.ToUInt32(columns[7]),
                    H_AMOUNT = Convert.ToDouble(columns[8])
                };

                int i = lineNum++ / workloadCountPerWorker;

                TPCCPaymentWorkload pmw = new TPCCPaymentWorkload(pm/*, this.tpccWorkers[i].vdb, this.tpccWorkers[i].redisClient*/);
                this.tpccWorkers[i].AddWorkload(pmw);

                if (lineNum == workloadTotal) break;
            }
            if (lineNum != workloadTotal) throw new Exception("there is no enough workload");
        }

        public void Run()
        {
            Console.WriteLine("Running TPCC workload...");
            this.startTicks = DateTime.Now.Ticks;

            Thread[] threads = new Thread[workerCount];
            for (int i = 0; i < this.workerCount; i++)
            {
                threads[i] = new Thread(new ThreadStart(tpccWorkers[i].Run));
                threads[i].Start();
            }

            foreach (Thread thread in threads)
            {
                thread.Join();
            }

            this.endTicks = DateTime.Now.Ticks;
        }

        internal int Throughput
        {
            get
            {
                double seconds = (this.endTicks - this.startTicks) / 10000000.0;
                int workloadTotal = this.workerCount * this.workloadCountPerWorker;
                Console.WriteLine("Processed {0} workloads in {1} seconds", workloadTotal, seconds);
                return (int)(workloadTotal / seconds);
            }
        }

    }
}
