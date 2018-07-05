using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GraphView.Transaction;
using Newtonsoft.Json;
using ServiceStack.Redis;

namespace TransactionBenchmarkTest.TPCC
{
    class TPCCAsyncBenchmark
    {
        private List<TransactionExecutor> executorList;

        private int workerCount;
        private int workloadCountPerWorker;

        //private TPCCWorker[] tpccWorkers;

        private long startTicks;
        private long endTicks;

        // redis command total
        private long commandCount = 0;

        // redis version db
        private RedisVersionDb redisVersionDb;

        private List<List<Tuple<string, int>>> partitionedInstances;

        internal int TxThroughput
        {
            get
            {
                int txCount = this.workloadCountPerWorker * this.workerCount;
                double runSeconds = this.RunSeconds;
                return (int)(txCount / runSeconds);
            }
        }

        internal double RunSeconds
        {
            get
            {
                return ((this.endTicks - this.startTicks) * 1.0) / 10000000;
            }
        }

        internal double RedisThroughput
        {
            get
            {
                double runSeconds = this.RunSeconds;
                return (int)(this.commandCount / runSeconds);
            }
        }

        public TPCCAsyncBenchmark(int workerCount, int workloadCountPerWorker, List<List<Tuple<string, int>>> instances = null)
        {
            this.workerCount = workerCount;
            this.workloadCountPerWorker = workloadCountPerWorker;
            this.redisVersionDb = RedisVersionDb.Instance();

            this.executorList = new List<TransactionExecutor>();

            if (instances == null || instances.Count > workerCount)
            {
                throw new ArgumentException("instances mustn't be null and the size should be smaller or equal to executorCount");
            }
            this.partitionedInstances = instances;
        }

        public void LoadNewOrderWorkload(string filepath)
        {
            Console.WriteLine("Loading New-Order workload");
            var csvReader = new System.IO.StreamReader(filepath);
            int workloadTotal = workerCount * workloadCountPerWorker;
            string line = null;
            int lineNum = 0;
            line = csvReader.ReadLine();    // ignore the first line

            int instanceIndex = 0;
            for (int i = 0; i < workerCount; i++)
            {
                Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();

                for (int k = 0; k < workloadCountPerWorker; k++)
                {
                    line = csvReader.ReadLine();
                    lineNum++;
                    if (line == null)
                    {
                        throw new Exception("there is no enough workload");
                    }

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
                    
                    TPCCNewOrderStoredProcedure nosp = new TPCCNewOrderStoredProcedure(lineNum.ToString(), no);
                    // TODO: should adopt to the lastest storedprocedure rule
                    //TransactionRequest req = new TransactionRequest(lineNum.ToString(), nosp);
                    TransactionRequest req = null;
                    reqQueue.Enqueue(req);
                }

                List<Tuple<string, int>> executorInstances = instanceIndex >= this.partitionedInstances.Count ? null :
                   this.partitionedInstances[instanceIndex++];
                this.executorList.Add(new TransactionExecutor(this.redisVersionDb, null, reqQueue, i, i));
            }
        }

        public void LoadPaymentWorkload(string filepath)
        {
            Console.WriteLine("Loading PAYMENT workload...");
            var csvReader = new System.IO.StreamReader(filepath);
            string line = null;
            int lineNum = 0;
            line = csvReader.ReadLine();    // ignore the first line: header

            int workloadTotal = workerCount * workloadCountPerWorker;

            int instanceIndex = 0;
            for (int i = 0; i < workerCount; i++)
            {
                RedisClient redisClient = new RedisClient(Constants.RedisHost, Constants.RedisPort);   // for payment to create `c_last` index
                redisClient.ChangeDb(Constants.RedisIndexDbN);

                Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();
                for (int k = 0; k < workloadCountPerWorker; k++)
                {
                    line = csvReader.ReadLine();
                    lineNum++;
                    if (line == null)
                    {
                        throw new Exception("there is no enough workload");
                    }

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

                    TPCCPaymentStoredProcedure pmsp = new TPCCPaymentStoredProcedure(lineNum.ToString(), pm, redisClient);
                    // TODO: should adopt to the lastest storedprocedure rule
                    // TransactionRequest req = new TransactionRequest(lineNum.ToString(), pmsp);
                    TransactionRequest req = null;
                    reqQueue.Enqueue(req);
                }

                List<Tuple<string, int>> executorInstances = instanceIndex >= this.partitionedInstances.Count ? null :
                   this.partitionedInstances[instanceIndex++];
                this.executorList.Add(new TransactionExecutor(this.redisVersionDb, null, reqQueue, i, i));
            }
        }

        public void Run(string tpccType)
        {
            Console.WriteLine("TPCC {0}", tpccType);
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.workerCount * this.workloadCountPerWorker), this.workerCount);
            Console.WriteLine("Running......");

            long commandCountBeforeRun = this.GetCurrentCommandCount();

            this.startTicks = DateTime.Now.Ticks;
            List<Thread> threadList = new List<Thread>();

            foreach (TransactionExecutor executor in this.executorList)
            {
                Thread thread = new Thread(new ThreadStart(executor.Execute));
                threadList.Add(thread);
                thread.Start();
            }

            while (true)
            {
                // check whether all tasks finished every 100 ms
                //Thread.Sleep(100);

                bool allFinished = true;
                foreach (TransactionExecutor executor in this.executorList)
                {
                    if (!executor.AllRequestsFinished)
                    {
                        allFinished = false;
                        break;
                    }
                }

                // Shutdown all workers
                if (allFinished)
                {
                    foreach (TransactionExecutor executor in this.executorList)
                    {
                        executor.Active = false;
                    }
                    break;
                }
            }

            this.endTicks = DateTime.Now.Ticks;

            long commandCountAfterRun = this.GetCurrentCommandCount();
            this.commandCount = commandCountAfterRun - commandCountBeforeRun;

            Console.WriteLine("Finished all tasks");
        }

        internal void Stats()
        {
            Console.WriteLine("\nFinshed {0} requests in {1} seconds", (this.workerCount * this.workloadCountPerWorker), this.RunSeconds);
            Console.WriteLine("Transaction Throughput: {0} tx/second", this.TxThroughput);

            int totalTxs = 0, abortedTxs = 0;
            foreach (TransactionExecutor executor in this.executorList)
            {
                totalTxs += executor.FinishedTxs;
                abortedTxs += (executor.FinishedTxs - executor.CommittedTxs);
            }
            Console.WriteLine("\nFinshed {0} txs, Aborted {1} txs", totalTxs, abortedTxs);
            Console.WriteLine("Transaction AbortRate: {0}%", (abortedTxs * 1.0 / totalTxs) * 100);

            Console.WriteLine("\nFinshed {0} commands in {1} seconds", this.commandCount, this.RunSeconds);
            Console.WriteLine("Redis Throughput: {0} cmd/second", this.RedisThroughput);

            Console.WriteLine();
        }

        private long GetCurrentCommandCount()
        {
            RedisClientManager manager = this.redisVersionDb.RedisManager;

            long commandCount = 0;
            for (int i = 0; i < manager.RedisInstanceCount; i++)
            {
                using (RedisClient redisClient = manager.GetLastestClient(0, 0))
                {
                    string countStr = redisClient.Info["total_commands_processed"];
                    long count = Convert.ToInt64(countStr);
                    commandCount += count;
                }
            }
            return commandCount;
        }
    }

}