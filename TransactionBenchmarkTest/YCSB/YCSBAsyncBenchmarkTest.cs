namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;

    class YCSBAsyncBenchmarkTest
    {
        public static readonly String TABLE_ID = "ycsb_table";

        public static readonly long REDIS_DB_INDEX = 7L;

        public static Func<object, object> ACTION = (object obj) =>
        {
            Tuple<VersionDb, TxWorkload> tuple = (Tuple<VersionDb, TxWorkload>)obj;

            TxWorkload workload = tuple.Item2;
            VersionDb versionDb = tuple.Item1;

            Transaction tx = new Transaction(null, versionDb);
            string readValue = null;
            switch (workload.Type)
            {
                case "READ":
                    readValue = (string)tx.Read(workload.TableId, workload.Key);
                    break;

                case "UPDATE":
                    readValue = (string)tx.Read(workload.TableId, workload.Key);
                    if (readValue != null)
                    {
                        tx.Update(workload.TableId, workload.Key, workload.Value);
                    }
                    break;

                case "DELETE":
                    readValue = (string)tx.Read(workload.TableId, workload.Key);
                    if (readValue != null)
                    {
                        tx.Delete(workload.TableId, workload.Key);
                    }
                    break;

                case "INSERT":
                    readValue = (string)tx.ReadAndInitialize(workload.TableId, workload.Key);
                    if (readValue == null)
                    {
                        tx.Insert(workload.TableId, workload.Key, workload.Value);
                    }
                    break;

                default:
                    break;
            }
            return true;
        };

        private List<TransactionExecutor> executorList;

        /// <summary>
        /// The number of executors
        /// </summary>
        private int executorCount;

        /// <summary>
        /// The number of tx count per executor
        /// </summary>
        private int txCountPerExecutor;

        /// <summary>
        /// The exact ticks when the test starts
        /// </summary>
        private long testBeginTicks;

        /// <summary>
        /// The exact ticks when then test ends
        /// </summary>
        private long testEndTicks;

        /// <summary>
        /// total redis commands processed
        /// </summary>
        private long commandCount = 0;

        /// <summary>
        /// the version db instance
        /// </summary>
        private VersionDb versionDb;

        /// <summary>
        /// the number of records need to be loaded
        /// </summary>
        private int recordCount;

        /// <summary>
        /// the total number of tasks
        /// </summary>
        private int totalTasks = 0;

        /// <summary>
        /// The partitioned instances to flush
        /// </summary>
        private List<List<Tuple<string, int>>> partitionedInstances;

        internal int TxThroughput
        {
            get
            {
                double runSeconds = this.RunSeconds;
                return (int)(this.totalTasks / runSeconds);
            }
        }

        internal double RunSeconds
        {
            get
            {
                return ((this.testEndTicks - this.testBeginTicks) * 1.0) / 10000000;
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

        internal TxResourceManager resourceManager;

        public YCSBAsyncBenchmarkTest(
            int recordCount,
            int executorCount, 
            int txCountPerExecutor, 
            VersionDb versionDb, 
            List<List<Tuple<string, int>>> instances = null,
            TxResourceManager resourceManager = null)
        {
            this.versionDb = versionDb;
            this.recordCount = recordCount;
            this.executorCount = executorCount;
            this.txCountPerExecutor = txCountPerExecutor;
            this.executorList = new List<TransactionExecutor>();
            this.totalTasks = 0;

            if (instances == null || instances.Count > executorCount)
            {
                throw new ArgumentException("instances mustn't be null and the size should be smaller or equal to executorCount");
            }
            this.partitionedInstances = instances;

            this.resourceManager = resourceManager;
        }

        internal void Setup(string dataFile, string operationFile)
        {
            // step1: flush the database
            this.versionDb.Clear();
            Console.WriteLine("Flushed the database");

            // step2: create version table
            this.versionDb.CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);

            // step3: load data
            // this.loadDataParallely(dataFile);
            // this.LoadDataSequentially(dataFile);

            // step 4: fill workers' queue
            this.FillWorkerQueue(operationFile);
        }

        internal void Run()
        {
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.executorCount * this.txCountPerExecutor), this.executorCount);
            Console.WriteLine("Running......");

            long commandCountBeforeRun = 0;
            if (this.versionDb is RedisVersionDb)
            {
                commandCountBeforeRun = this.GetCurrentCommandCount();
            }

            this.testBeginTicks = DateTime.Now.Ticks;
            List<Thread> threadList = new List<Thread>();

            foreach (TransactionExecutor executor in this.executorList)
            {
                Thread thread = new Thread(new ThreadStart(executor.ExecuteNoFlush));
                threadList.Add(thread);
                thread.Start();
            }

            int finishedTasks = 0;
            while (true)
            {
                // check whether all tasks finished every 100 ms
                Thread.Sleep(100);
                finishedTasks = 0;
                bool allFinished = true;

                foreach (TransactionExecutor executor in this.executorList)
                {
                    if (!executor.AllRequestsFinished)
                    {
                        allFinished = false;
                        finishedTasks += executor.FinishedTxs;
                    }
                }
                Console.WriteLine("Execute {0} Tasks", finishedTasks);
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

            this.testEndTicks = DateTime.Now.Ticks;

            if (this.versionDb is RedisVersionDb)
            {
                long commandCountAfterRun = this.GetCurrentCommandCount();
                this.commandCount = commandCountAfterRun - commandCountBeforeRun;
            }

            Console.WriteLine("Finished all tasks");
        }

        internal void Stats()
        {
            Console.WriteLine("\nFinshed {0} requests in {1} seconds", (this.executorCount * this.txCountPerExecutor), this.RunSeconds);
            Console.WriteLine("Transaction Throughput: {0} tx/second", this.TxThroughput);

            int totalTxs = 0, abortedTxs = 0;
            foreach (TransactionExecutor executor in this.executorList)
            {
                totalTxs += executor.FinishedTxs;
                abortedTxs += (executor.FinishedTxs - executor.CommittedTxs);
            }
            Console.WriteLine("\nFinshed {0} txs, Aborted {1} txs", totalTxs, abortedTxs);
            Console.WriteLine("Transaction AbortRate: {0}%", (abortedTxs * 1.0 / totalTxs) * 100);

            if (this.versionDb is RedisVersionDb)
            {
                Console.WriteLine("\nFinshed {0} commands in {1} seconds", this.commandCount, this.RunSeconds);
                Console.WriteLine("Redis Throughput: {0} cmds/second", this.RedisThroughput);
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Load record by multiple threads
        /// </summary>
        /// <param name="dataFile"></param>
        private void loadDataParallely(string dataFile)
        {
            // 3.1 compute the number of partitions
            int partitions = 0;
            if (this.versionDb is RedisVersionDb)
            {
                partitions = ((RedisVersionDb)this.versionDb).RedisManager.RedisInstanceCount;
            }
            else if (this.versionDb is SingletonPartitionedVersionDb)
            {
                partitions = ((SingletonPartitionedVersionDb)this.versionDb).PartitionCount;
            }

            // 3.2 fill the flushedInstances
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>(partitions);
            for (int partition = 0; partition < partitions; partition++)
            {
                instances.Add(new List<Tuple<string, int>>()
                {
                    Tuple.Create(VersionDb.TX_TABLE, partition),
                    Tuple.Create(TABLE_ID, partition)
                });
            }

            // 3.3 Load in multiple workers
            int workerCount = Math.Max(2, partitions), taskPerWorker = this.recordCount / workerCount;
            List<TransactionExecutor> executors = new List<TransactionExecutor>();
            using (StreamReader reader = new StreamReader(dataFile))
            {
                string line;
                int count = 0;
                for (int i = 0; i < workerCount; i++)
                {
                    Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();
                    for (int j = 0; j < taskPerWorker; j++)
                    {
                        line = reader.ReadLine();
                        string[] fields = this.ParseCommandFormat(line);
                        count++;

                        TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                        string sessionId = ((i * this.txCountPerExecutor) + j + 1).ToString();
                        YCSBStoredProcedure procedure = new YCSBStoredProcedure(sessionId, workload);
                        TransactionRequest req = new TransactionRequest(sessionId, procedure);
                        reqQueue.Enqueue(req);
                    }
                    List<Tuple<string, int>> executorInstances =
                        i >= instances.Count ?
                        null :
                        instances[i];

                    executors.Add(new TransactionExecutor(this.versionDb, null, reqQueue, executorInstances, i, 0, this.resourceManager));
                }
            }

            // 3.4 load records
            //List<Thread> threadList = new List<Thread>();
            //foreach (TransactionExecutor executor in executors)
            //{
            //    Thread thread = new Thread(new ThreadStart(executor.Execute));
            //    threadList.Add(thread);
            //    thread.Start();
            //}

            //int loaded = 0, times = 0;
            //while (true)
            //{
            //    // check whether all tasks finished every 100 ms
            //    Thread.Sleep(100);
            //    times++;

            //    bool allFinished = true;
            //    loaded = 0;
            //    foreach (TransactionExecutor executor in executors)
            //    {
            //        if (!executor.AllRequestsFinished)
            //        {
            //            allFinished = false;
            //        }
            //        loaded += executor.FinishedTxs;
            //    }

            //    Console.WriteLine("Loaded {0} records", loaded);

            //    // Console.WriteLine("Loaded {0} records", loaded);
            //    // Shutdown all workers
            //    if (allFinished)
            //    {
            //        foreach (TransactionExecutor executor in executors)
            //        {
            //            executor.Active = false;
            //        }
            //        break;
            //    }
            //}

            int loaded = 0;
            Console.WriteLine("Load records successfully, {0} records in total", loaded);
            Console.WriteLine("END");
        }

        private void LoadDataSequentially(string dataFile)
        {
            using (StreamReader reader = new StreamReader(dataFile))
            {
                string line;
                int count = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseCommandFormat(line);
                    TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                    count++;

                    ACTION(Tuple.Create(this.versionDb, workload));
                    if (count % 10000 == 0)
                    {
                        Console.WriteLine("Loaded {0} records", count);
                    }
                }
                Console.WriteLine("Load records successfully, {0} records in total", count);
            }
        }

        private void FillWorkerQueue(string operationFile)
        {
            using (StreamReader reader = new StreamReader(operationFile))
            {
                string line;
                int instanceIndex = 0;
                for (int i = 0; i < this.executorCount; i++)
                {
                    Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();
                    for (int j = 0; j < this.txCountPerExecutor; j++)
                    {
                        line = reader.ReadLine();
                        string[] fields = this.ParseCommandFormat(line);

                        TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                        string sessionId = ((i * this.txCountPerExecutor) + j + 1).ToString();
                        YCSBStoredProcedure procedure = new YCSBStoredProcedure(sessionId, workload);
                        TransactionRequest req = new TransactionRequest(sessionId, procedure);
                        reqQueue.Enqueue(req);
                    }

                    this.totalTasks += reqQueue.Count;
                    List<Tuple<string, int>> executorInstances =
                        instanceIndex >= this.partitionedInstances.Count ?
                        null :
                        this.partitionedInstances[instanceIndex++];

                    this.executorList.Add(new TransactionExecutor(this.versionDb, null, reqQueue, executorInstances, i, 0, this.resourceManager));
                }
            }
        }

        private string[] ParseCommandFormat(string line)
        {
            string[] fields = line.Split(' ');
            string value = null;
            int fieldsOffset = fields[0].Length + fields[1].Length + fields[2].Length + 3 + 9;
            int fieldsEnd = line.Length - 2;

            if (fieldsOffset < fieldsEnd)
            {
                value = line.Substring(fieldsOffset, fieldsEnd - fieldsOffset + 1);
            }

            return new string[] {
                fields[0], fields[1], fields[2], value
            };
        }

        /// <summary>
        /// Here there is a bug in ServiceStack.Redis, it will not refresh the info aftering reget a client from
        /// the pool, which means the number of commands will be not changed.
        /// 
        /// The bug has been fixed in Version 4.0.58 (Commerical Version), our reference version is Version 3.9 (The last open source version).
        /// 
        /// So here we have to dispose the redis client manager and reconnect with redis to get the lastest commands.
        /// </summary>
        /// <returns></returns>
        private long GetCurrentCommandCount()
        {
            RedisVersionDb redisVersionDb = (RedisVersionDb)this.versionDb;
            RedisClientManager manager = redisVersionDb.RedisManager;

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
