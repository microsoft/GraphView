namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    class YCSBAsyncBenchmarkTest
    {
        /// <summary>
        /// Only for benchmark test
        /// </summary>
        public static readonly String TABLE_ID = "ycsb_table";

        public static readonly long REDIS_DB_INDEX = 7L;

        public static readonly int RANGE_OFFSET_PER_PROCESS = 8;

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
        /// the tables should be flushed
        /// </summary>
        private string[] tables;

        private BenchmarkTestConfig config;

        //// stable round setting
        private long stableStartTicks = -1;
        private long stableEndTicks = -1;
        private int stableRoundStart = -1;
        private int stableRoundEnd = -1;
        private int stableStartFinished = 0;
        private int stableEndFinished = 0;

        public void SetStableRound(int s, int e)
        {
            this.stableRoundStart = s;
            this.stableRoundEnd = e;
        }

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

        public YCSBAsyncBenchmarkTest(
            int recordCount,
            int executorCount,
            int txCountPerExecutor,
            VersionDb versionDb,
            string[] flushTables = null,
            BenchmarkTestConfig config = null)
        {
            this.versionDb = versionDb;
            this.recordCount = recordCount;
            this.executorCount = executorCount;
            this.txCountPerExecutor = txCountPerExecutor;
            this.executorList = new List<TransactionExecutor>();
            this.totalTasks = 0;
            this.tables = flushTables;
            
            if (config == null)
            {
                config = new BenchmarkTestConfig();
            }
            this.config = config;

            Console.WriteLine("Current Benchmark Test Config: {0}", this.config.ToString());
            Console.WriteLine("Wait for 5 seconds to confirm");
            Console.Out.Flush();
            //Thread.Sleep(5000);
        }

        internal void Setup(string dataFile, string operationFile)
        {
            // step1: flush the database
            if (config.ClearVersionDb)
            {
                this.versionDb.Clear();
            }

            Console.WriteLine("cleared the database");

            // step2: create version table
            this.versionDb.CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);

            //if (this.versionDb is SingletonPartitionedVersionDb)
            //{
            //    ((SingletonPartitionedVersionDb)this.versionDb).StartDaemonThreads();
            //}

            // step3: mock load data
            if (this.versionDb is SingletonVersionDb ||
                this.versionDb is RedisVersionDb)
            {
                if (config.LoadRecords)
                {
                    this.versionDb.MockLoadData(this.recordCount);
                }
            }

            // step 4: fill workers' queue
            this.executorList = this.MockFillYCSBWorkload();
        }

        //This method is for SingletonVersionDb only.
        internal void ResetAndFillWorkerQueue(string operationFile, int currentExecutorCount)
        {            
            // fill workers' queue
            foreach (TransactionExecutor executor in this.executorList)
            {
                executor.Reset();
            }

            List<TransactionExecutor> appendedExecutors = null;
            appendedExecutors = this.MockFillYCSBWorkload(this.executorCount, currentExecutorCount - this.executorCount);
            this.executorCount = currentExecutorCount;
            this.executorList.AddRange(appendedExecutors);
        }

        internal void Run()
        {
            if (!config.RunTest)
            {
                Console.WriteLine("No running commands");
                return;
            }

            /// Start GC to collect memory before running
            Console.WriteLine("Memory used before collection:       {0:N0} MB",
                              GC.GetTotalMemory(false) / (1024 * 1024.0));

            Console.WriteLine("Waiting GC");
            GC.Collect();
            Console.WriteLine("Memory used after full collection:   {0:N0} MB",
                              GC.GetTotalMemory(false) / (1024 * 1024.0));

            VersionDb.EnqueuedRequests = 0;

            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.executorCount * this.txCountPerExecutor), this.executorCount);
            Console.WriteLine("Running......");

            long commandCountBeforeRun = 0;
            if (this.versionDb is RedisVersionDb)
            {
                commandCountBeforeRun = this.GetCurrentCommandCount();
            }

            Task[] tasks = new Task[this.executorCount];
            int tid = 0;
            foreach (TransactionExecutor executor in this.executorList)
            {
                if (this.versionDb is SingletonVersionDb || this.versionDb is SingletonPartitionedVersionDb)
                {
                    tasks[tid] = Task.Factory.StartNew(executor.YCSBExecuteUpdate);
                }
                else if (this.versionDb is RedisVersionDb)
                {
                    tasks[tid] = Task.Factory.StartNew(executor.ExecuteWithoutWorkload);
                }
                
                tid++;
            }

            this.testBeginTicks = DateTime.Now.Ticks;

            Task.WaitAll(tasks);

            this.testEndTicks = DateTime.Now.Ticks;

            // stop monitors
            if (this.versionDb is PartitionedCassandraVersionDb)
            {
                (this.versionDb as PartitionedCassandraVersionDb).StopMonitors();
            }
           
            if (this.versionDb is RedisVersionDb)
            {
                long commandCountAfterRun = this.GetCurrentCommandCount();
                this.commandCount = commandCountAfterRun - commandCountBeforeRun;
            }
            
            Console.WriteLine("Finished all tasks");
        }

        internal void Stats()
        {
            this.totalTasks = this.executorCount * this.txCountPerExecutor;
            Console.WriteLine("\nWay1 to Compute Throughput");
            Console.WriteLine("\nFinshed {0} requests in {1} seconds", (this.executorCount * this.txCountPerExecutor), this.RunSeconds);
            Console.WriteLine("Transaction Throughput: {0} tx/second", this.TxThroughput);

            Console.WriteLine("\nWay2 to Compute Throughput");
            int executorId = 0;
            double totalRunSeconds = 0;
            int realFinishedTasks = 0;
            foreach (TransactionExecutor executor in this.executorList)
            {
                realFinishedTasks += executor.FinishedTxs;
                double runSeconds = (executor.RunEndTicks - executor.RunBeginTicks) * 1.0 / 10000000;
                totalRunSeconds += runSeconds;
                Console.WriteLine("Executor {0} run time: {1}s", executorId++, runSeconds);
            }

            double averageRunSeconds = totalRunSeconds / this.executorCount;
            //double throughput2 = this.totalTasks / averageRunSeconds;
            double throughput2 = realFinishedTasks / averageRunSeconds;
            // Console.WriteLine("\nFinshed {0} requests in {1} seconds", (this.executorCount * this.txCountPerExecutor), this.RunSeconds);
            Console.WriteLine("Transaction Throughput: {0} tx/second", (int)throughput2);

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

            Console.WriteLine("Enqueued Tx Requests Count: {0}", VersionDb.EnqueuedRequests);
            Console.WriteLine();
        }

        // do some init work
        internal void StartMonitors()
        {
            if (this.versionDb is PartitionedCassandraVersionDb)
            {
                this.versionDb.CreateVersionTable(TABLE_ID);
                (this.versionDb as PartitionedCassandraVersionDb).StartMonitors();
            }
        }

        internal void SetupOps(string operationFile)
        {
            // step1: flush the database
            this.versionDb.ClearTxTable();
            Console.WriteLine("Flushed the tx database");

            this.executorList = this.FillWorkerQueue(operationFile);
        }

        internal void SetupOpsNull(int indexBound = 200000)
        {
            // step1: flush the database
            this.versionDb.ClearTxTable();
            Console.WriteLine("Flushed the tx database");

            this.totalTasks = this.executorCount * this.txCountPerExecutor;

            List<TransactionExecutor> executors = new List<TransactionExecutor>();
            for (int i = 0; i < this.executorCount; i++)
            {
                int partition_index = i % this.versionDb.PartitionCount;
                //executors.Add(new TransactionExecutor(this.versionDb, null, null, partition_index, i, 0,
                //    this.versionDb.GetResourceManager(partition_index), tables, null, null, this.recordCount, this.txCountPerExecutor));
                executors.Add(new TransactionExecutor(this.versionDb, null, null, partition_index, i, 0,
                    null, tables, null, null, this.recordCount, this.txCountPerExecutor));
                //Console.WriteLine("i={0}", i);
            }
            this.executorList = executors;
        }

        class DataLoadWorker
        {
            private VersionDb db;
            private int start;
            private int end;
            private YCSBAsyncBenchmarkTest test;
            private int sum = 0;
            private volatile bool isFinished = false;

            public DataLoadWorker(VersionDb db, int start, int end, YCSBAsyncBenchmarkTest test)
            {
                this.db = db;
                this.start = start;
                this.end = end;
                this.test = test;
            }

            public int getCount()
            {
                return sum;
            }

            public bool getIsFinished()
            {
                return this.isFinished;
            }

            public void load()
            {
                test.loadWithOneThread(db, start, end , ref sum);
                isFinished = true;
            }

        }


        internal void load(VersionDb db, int threadCount, int totalRecord)
        {
            int interval = totalRecord / threadCount;
            List<Thread> threadList = new List<Thread>();

            int[] count = new int[threadCount];
            List<DataLoadWorker> loadList = new List<DataLoadWorker>();

            for (int i = 0; i < threadCount; i++)
            {
                DataLoadWorker load = new DataLoadWorker(db, interval * i,
                    Math.Min(interval * (i + 1), totalRecord), this);
                Thread t = new Thread(new ThreadStart(load.load));
                loadList.Add(load);
                threadList.Add(t);
                t.Start();
            }

            long lastTime = DateTime.Now.Ticks;
            long now;
            int lastSum = 0;
            int sum = 0;
            int thread = 0;
            foreach (DataLoadWorker loader in loadList)
            {
                lastSum += loader.getCount();
            }

            bool isFinished = false;

            while (!isFinished)
            {
                Thread.Sleep(1000);
                sum = 0;
                isFinished = true;
                thread = 0;
                foreach (DataLoadWorker loader in loadList)
                {
                    sum += loader.getCount();
                    if (!loader.getIsFinished())
                    {
                        thread++;
                        isFinished = false;
                    }
                }
                now = DateTime.Now.Ticks;
                long runSeconds = (now - lastTime) / 10000000;
                Console.WriteLine("Time Inteval : {0}, Total Finish Count: {1}, Throughput: {2} txn/s, thread : {3}", 
                    runSeconds, sum, (sum - lastSum) / runSeconds, thread);
                lastSum = sum;
                lastTime = now;
            }

            foreach (Thread t in threadList)
            {
                t.Join();
            }

        }

        internal void loadWithOneThread(VersionDb db, int start, int end, ref int sum)
        {
            int batchSize = 1;

            for (int i = start; i < end; i += batchSize)
            {
                sum += batchInsert(db, i, Math.Min(end, i + batchSize));
            }
        }

        internal int batchInsert(VersionDb db, int start, int end)
        {
            int count = 0;
            Transaction txn = new Transaction(null, db);
            string readValue = null;
            for (int i = start; i < end; i++)
            {
                readValue = (string)txn.ReadAndInitialize(TABLE_ID, i);
                if (readValue == null)
                {
                    txn.Insert(TABLE_ID, i, new String('a', 100));
                    count++;
                }
            }
            txn.Commit();
            return count;
        }

        private List<TransactionExecutor> fillYCSBWorkLoad()
        {
            List<TransactionExecutor> executorList = new List<TransactionExecutor>();

            IDataGenerator generator = this.BuildDataGenerator();
            var workloadAction = this.BuildWorkloadAction(generator);
            StoredProcedureWorkload.Reload = workloadAction;
            StoredProcedureType type = this.GetStoredProcedureType(config.Type);

            this.totalTasks = this.txCountPerExecutor * executorCount;

            for (int i = 0; i < executorCount; i++)
            {
                int partition_index = i % this.versionDb.PartitionCount;
                Queue<TransactionRequest> requestQueue = new Queue<TransactionRequest>();
                for (int j = 0; j < this.txCountPerExecutor; j++)
                {
                    StoredProcedureWorkload workload = GetStoredProcedureWorkload(config.Type,config.QueryCount);
                    StoredProcedureWorkload.Reload(workload);
                    string sessionId = (i * this.txCountPerExecutor + j + 1).ToString();
                    TransactionRequest request = new TransactionRequest(sessionId, workload,StoredProcedureType.YCSBStordProcedure);
                    requestQueue.Enqueue(request);
                }
                executorList.Add(new TransactionExecutor(this.versionDb, null, requestQueue, partition_index, 
                    i, 0, null, tables, null, null, this.recordCount, this.txCountPerExecutor, null, null,
                    type, config.PipelineSize));
            }
            return executorList;
        }

        internal void prepare()
        {
            if (config.ClearVersionDb)
            {
                this.versionDb.Clear();
            }

            this.versionDb.CreateVersionTable(TABLE_ID);
            this.executorList = fillYCSBWorkLoad();
        }


        // for PartitionedCassandra
        internal void Run2(string exeType = "ycsb_sync_ro_intk")
        {
            Console.WriteLine("**** Execute Type: " + exeType + " ****");
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.executorCount * this.txCountPerExecutor), this.executorCount);

            //  create multi threads
            List<Thread> threadList = new List<Thread>(this.executorCount);
            foreach (TransactionExecutor executor in this.executorList)
            {
                Thread thread = null;

                switch(exeType)
                {
                    case "ycsb_sync_ro_strk":
                        thread = new Thread(executor.YCSBExecuteRead2);
                        break;
                    ///
                    case "ycsb_sync_ro_intk":
                        thread = new Thread(executor.YCSBExecuteRead3);
                        break;
                    case "ycsb_sync_wo_intk":
                        thread = new Thread(executor.YCSBExecuteUpdate3);
                        break;
                    case "ycsb_sync_rw_intk":
                        thread = new Thread(executor.YCSBExecuteReadUpdateHybrid);
                        break;
                    ///
                    case "async":
                        thread = new Thread(executor.Execute2);
                        break;
                    // test cassandra throughput only
                    case "cassandra_ro_intk":
                        thread = new Thread(executor.CassandraReadOnly);
                        break;
                    case "cassandra_wo_intk":
                        thread = new Thread(executor.CassandraUpdateOnly);
                        break;
                }

                threadList.Add(thread);
            }

            // start
            Console.WriteLine("Ready to start threads");
            
            this.testBeginTicks = DateTime.Now.Ticks;

            foreach(Thread t in threadList)
            {
                t.Start();
            }

            Console.WriteLine("Running......");

            int delta_t = 1;
            int round = 0;
            int lastTimeTotal = 0;
            int lastTimeCommit = 0;
            while (true)
            {
                Thread.Sleep(delta_t*1000);     // sleep 1s
                round++;

                int total = 0;
                int commit = 0;
                bool allok = true;

                foreach (TransactionExecutor executor in this.executorList)
                {
                    total += executor.FinishedTxs;
                    commit += executor.CommittedTxs;
                    allok &= executor.AllRequestsFinished;
                }
                if (round == this.stableRoundStart)
                {
                    this.stableStartTicks = DateTime.Now.Ticks;
                    this.stableStartFinished = total;
                }
                if (round == this.stableRoundEnd)
                {
                    this.stableEndTicks = DateTime.Now.Ticks;
                    this.stableEndFinished = total;
                    foreach (TransactionExecutor executor in this.executorList)
                    {
                        executor.Active = false;
                    }
                    break;
                }
                double abortRatioLocal = (total == lastTimeTotal ? 0 : 1 - 1.0 * (commit - lastTimeCommit) / (total - lastTimeTotal));
                Console.WriteLine("Elapsed ~{0}s, Finished {1} TXs, {2} Ops/s, Commit {3} , Abort : {4}", round * delta_t, total,
                    total - lastTimeTotal,(commit - lastTimeCommit), abortRatioLocal);
                lastTimeTotal = total;
                lastTimeCommit = commit;
                if (allok)
                {
                    break;
                }
            }
           
            this.testEndTicks = DateTime.Now.Ticks;

            // stop monitors
            if (this.versionDb is PartitionedCassandraVersionDb)
            {
                (this.versionDb as PartitionedCassandraVersionDb).StopMonitors();
            }

            int totalCommit = 0;

            foreach (TransactionExecutor executor in this.executorList)
            {
                totalCommit += executor.CommittedTxs;
                executor.Active = false;
            }
            
            Console.WriteLine("Finished !");
            long runSeconds = (this.testEndTicks - this.testBeginTicks) / 10000000;
            double abortRatio = (this.txCountPerExecutor * this.executorCount == 0 ? 0 : 1 - 1.0 * totalCommit / (this.txCountPerExecutor * this.executorCount)); 
            Console.WriteLine("Time Cost : {0}, Finish Count: {1}, Throughput : {2} txn/s , AbortRatio: {3}", runSeconds,
                this.txCountPerExecutor * this.executorCount, this.txCountPerExecutor * this.executorCount / runSeconds, abortRatio);
        }

        internal void Stats2()
        {
            int realFinishedTasks = 0;
            double totalRunSeconds = 0;
            int abortedTxs = 0;

            long minStartTicks = long.MaxValue;
            long maxStartTicks = long.MinValue;
            long minEndTicks = long.MaxValue;
            long maxEndTicks = long.MinValue;
            double thSum = 0;

            foreach (TransactionExecutor executor in this.executorList)
            {
                realFinishedTasks += executor.FinishedTxs;
                abortedTxs += (executor.FinishedTxs - executor.CommittedTxs);
                double runSeconds = (executor.RunEndTicks - executor.RunBeginTicks) * 1.0 / 10000000;
                totalRunSeconds += runSeconds;
                minStartTicks = Math.Min(minStartTicks, executor.RunBeginTicks);
                maxStartTicks = Math.Max(maxStartTicks, executor.RunBeginTicks);
                minEndTicks = Math.Min(minEndTicks, executor.RunEndTicks);
                maxEndTicks = Math.Max(maxEndTicks, executor.RunEndTicks);
                thSum += executor.FinishedTxs*1.0 / runSeconds;
            }

            double minMaxSeconds = (maxEndTicks - minStartTicks) * 1.0 / 10000000;
            double thMinMax = realFinishedTasks * 1.0 / minMaxSeconds;

            if (this.stableRoundStart < 0 || this.stableRoundEnd < 0)
            {
                double deltaSeconds = (this.testEndTicks - this.testBeginTicks)*1.0 / 10000000;
                double th = this.totalTasks / deltaSeconds;
                Console.WriteLine("==== Rough Throughput ====");
                Console.WriteLine("Transaction Throughput: {0} tx/s\n", th);
            }

            Console.WriteLine("==== Throughput delta = [min, max]");
            Console.WriteLine("Transaction Throughput: {0} tx/s\n", thMinMax);

            double averageRunSeconds = totalRunSeconds / this.executorCount;
            double throughput2 = realFinishedTasks / averageRunSeconds;
            Console.WriteLine("==== Average Throughput ====");
            Console.WriteLine("Transaction Throughput: {0} tx/second\n", (int)throughput2);

            Console.WriteLine("Finshed {0} txs, Aborted {1} txs", realFinishedTasks, abortedTxs);
            Console.WriteLine("Transaction AbortRate: {0}%\n", (abortedTxs * 1.0 / realFinishedTasks) * 100);

            if (this.stableRoundStart > 0 && this.stableRoundEnd > 0)
            {
                int delta1 = this.stableRoundEnd - this.stableRoundStart;
                int delta2 = this.stableEndFinished - this.stableStartFinished;
                double delta1Seconds = (this.stableEndTicks - this.stableStartTicks) * 1.0 / 10000000;
                Console.WriteLine("STABLE ROUND {0} seconds: throughput {1}/s", delta1, delta2 * 1.0 / delta1);
                Console.WriteLine("STABLE ROUND {0} seconds: throughput {1}/s", delta1, delta2 * 1.0 / delta1Seconds);
            }

            // NOTE: this method may be incorrect if there is no sharing time among threads
            Console.WriteLine("==== Throughput SUM of each worker ====");
            Console.WriteLine("MinEnd - MaxStart = {0}s", (minEndTicks - maxStartTicks) * 1.0 / 10000000);
            Console.WriteLine("Transaction Throughput: {0} tx/s\n", thSum);

            //(this.versionDb as PartitionedCassandraVersionDb).ShowLoadBalance();
            Console.WriteLine();
        }

        /// <summary>
        /// Load record by multiple threads
        /// </summary>
        /// <param name="dataFile"></param>
        private void LoadDataParallely(string dataFile)
        {
            long beginTicks = DateTime.Now.Ticks;
            // 3.1 compute the number of partitions
            int partitions = 0;
            if (this.versionDb is RedisVersionDb)
            {
                partitions = ((RedisVersionDb)this.versionDb).RedisManager.RedisInstanceCount;
            }
            else if (this.versionDb is SingletonPartitionedVersionDb)
            {
                partitions = ((SingletonPartitionedVersionDb)this.versionDb).PartitionCount;
            } else if (this.versionDb is PartitionedCassandraVersionDb)
            {
                partitions = ((PartitionedCassandraVersionDb)this.versionDb).PartitionCount;
            }
            Console.WriteLine("Partitions Count: {0}", partitions);

            List<TransactionExecutor> executors;
            // 3.2 Load in multiple workers
            executors = this.FillWorkerQueue(dataFile);

            // 3.4 load records
            Task[] tasks = new Task[executors.Count];
            int tid = 0;
            foreach (TransactionExecutor executor in executors)
            {
                //tasks[tid++] = Task.Factory.StartNew(executor.ExecuteInSync);
                tasks[tid++] = Task.Factory.StartNew(executor.Execute2);
            }
            Task.WaitAll(tasks);

            foreach (TransactionExecutor executor in executors)
            {
                executor.Active = false;
            }

            long endTicks = DateTime.Now.Ticks;
            Console.WriteLine("Load records successfully, {0} records in total in {1} seconds",
                this.recordCount, (endTicks - beginTicks) * 1.0 / 10000000);
            Console.WriteLine("END");
        }

        private void LoadDataSequentially(string dataFile)
        {
            long beginTicks = DateTime.Now.Ticks;

            using (StreamReader reader = new StreamReader(dataFile))
            {
                string line;
                int count = 0;

                Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();

                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseCommandFormat(line);
                    YCSBWorkload workload = new YCSBWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                    count++;

                    string sessionId = count.ToString();
                    TransactionRequest req = new TransactionRequest(sessionId, workload, StoredProcedureType.YCSBStordProcedure);
                    reqQueue.Enqueue(req);
                    // ACTION(Tuple.Create(this.versionDb, workload));
                    if (count % 10000 == 0)
                    {
                        // Console.WriteLine("Loaded {0} records", count);
                        Console.WriteLine("Enqueued {0} tx insert request", count);
                    }
                    if (count == this.recordCount)
                    {
                        break;
                    }
                }
                
                TransactionExecutor executor = new TransactionExecutor(this.versionDb, null, reqQueue, 0, 0, 0,
                    null, this.tables);
                // new a thread to run the executor
                Thread thread = new Thread(new ThreadStart(executor.Execute2));
                thread.Start();
                thread.Join();
                //while (!executor.AllRequestsFinished)
                //{
                //    // Console.WriteLine("Loaded {0} records", executor.CommittedTxs);
                //    //if (executor.CommittedTxs > 0 && executor.CommittedTxs % 500 == 0)
                //    //{
                //    //    // Console.WriteLine("Loaded {0} records", count);
                //    //    Console.WriteLine("Executed {0} tx insert request", executor.CommittedTxs);
                //    //}
                //}
                Console.WriteLine("Load records successfully, {0} records in total", executor.CommittedTxs);
                executor.Active = false;
                // executor.RecycleTxTableEntryAfterFinished();
                thread.Abort();
            }

            long endTicks = DateTime.Now.Ticks;
            Console.WriteLine("Elapsed time {0} seconds", ((endTicks - beginTicks) * 1.0 / 10000000));
        }

        private List<TransactionExecutor> MockFillYCSBWorkload(int offset = 0, int limit = -1)
        {
            int appendCount = limit == -1 ? this.executorCount : limit;
            List<TransactionExecutor> executors = new List<TransactionExecutor>();

            IDataGenerator generator = this.BuildDataGenerator();
            var workloadAction = this.BuildWorkloadAction(generator);
            StoredProcedureWorkload.Reload = workloadAction;
            StoredProcedureType spType = this.GetStoredProcedureType(config.Type);

            for (int i = offset; i < offset + appendCount; i++)
            {
                Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();
                StoredProcedureWorkload workload = GetStoredProcedureWorkload(config.Type, config.QueryCount);

                this.totalTasks += this.txCountPerExecutor;
                int partition_index = i % this.versionDb.PartitionCount;

                int txRangeStart = i;
                if (config.MultiProcessMode)
                {
                    txRangeStart = i + config.ProcessOffset * RANGE_OFFSET_PER_PROCESS;
                }
                executors.Add(new TransactionExecutor(this.versionDb, null, reqQueue, partition_index, txRangeStart, 0,
                    null, tables, null, null, this.recordCount, this.txCountPerExecutor, null, workload, spType, config.PipelineSize));

                Console.WriteLine("Filled {0}-th executors", i+1);
            }

            return executors;
        }

        private List<TransactionExecutor> FillWorkerQueue(string operationFile)
        {
            List<TransactionExecutor> executors = new List<TransactionExecutor>();
            using (StreamReader reader = new StreamReader(operationFile))
            {
                string line;
                //int instanceIndex = 0;
                for (int i = 0; i < this.executorCount; i++)
                {
                    Queue<TransactionRequest> reqQueue = new Queue<TransactionRequest>();
                    for (int j = 0; j < this.txCountPerExecutor; j++)
                    {
                        line = reader.ReadLine();
                        string[] fields = this.ParseCommandFormat(line);
                        YCSBWorkload workload = new YCSBWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                        //Console.WriteLine("recordkey = {0}", fields[2]);
                        string sessionId = ((i * this.txCountPerExecutor) + j + 1).ToString();
                        TransactionRequest req = new TransactionRequest(sessionId, workload, StoredProcedureType.YCSBStordProcedure);
                        reqQueue.Enqueue(req);
                    }

                    //Console.WriteLine("Filled {0} executors", i + 1);

                    this.totalTasks += this.txCountPerExecutor;
                    int partition_index = i % this.versionDb.PartitionCount;
                    //executors.Add(new TransactionExecutor(this.versionDb, null, reqQueue, partition_index, i, 0,
                    //   null, tables, null, null, this.YCSBKeys, this.txCountPerExecutor));
                    executors.Add(new TransactionExecutor(this.versionDb, null, reqQueue, partition_index, i, 0,
                      null, tables, null, null, this.recordCount, this.txCountPerExecutor));
                }

                Console.WriteLine("Filled {0} executors", this.executorCount);

                return executors;
            }
        }

        // Reshuffle the workloads and make sure all workloads in the same executor
        // will have the same partition key
        private List<TransactionExecutor> ReshuffleFillWorkerQueue(string operationFile, int executorCount, int totalWorkloads)
        {
            // new transaction queues at first
            Queue<TransactionRequest>[] queueArray = new Queue<TransactionRequest>[executorCount];
            List<TransactionExecutor> executors = new List<TransactionExecutor>(executorCount);

            for (int i = 0; i < executorCount; i++)
            {
                queueArray[i] = new Queue<TransactionRequest>();
            }

            using (StreamReader reader = new StreamReader(operationFile))
            {
                string line;
                for (int i = 0; i < totalWorkloads; i++)
                {
                    line = reader.ReadLine();
                    string[] fields = this.ParseCommandFormat(line);

                    YCSBWorkload workload = new YCSBWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                    string sessionId = (i + 1).ToString();
                    TransactionRequest req = new TransactionRequest(sessionId, workload, StoredProcedureType.YCSBStordProcedure);

                    int pk = this.versionDb.PhysicalPartitionByKey(fields[2]);
                    queueArray[pk].Enqueue(req);
                }
            }

            for (int pk = 0; pk < executorCount; pk++)
            {
                Queue<TransactionRequest> txQueue = queueArray[pk];
                TxResourceManager manager = this.versionDb.GetResourceManager(pk);

                executors.Add(
                    new TransactionExecutor(this.versionDb, null, txQueue, pk, pk, 0, manager, tables));

                Console.WriteLine("Executor {0} workloads count: {1}", pk, txQueue.Count);
            }

            return executors;
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
            int testedInstances = redisVersionDb.PartitionCount / RedisVersionDb.PARTITIONS_PER_INSTANCE;
            if (redisVersionDb.Mode == RedisVersionDbMode.Cluster)
            {
                testedInstances = 1;
            }

            for (int i = 0; i < testedInstances; i++)
            {
                using (RedisClient redisClient = manager.GetLastestClient(0, i))
                {
                    string countStr = redisClient.Info["total_commands_processed"];
                    long count = Convert.ToInt64(countStr);
                    commandCount += count;
                }
            }
            return commandCount;
        }

        private Action<StoredProcedureWorkload> BuildWorkloadAction(IDataGenerator generator)
        {
            Action<StoredProcedureWorkload> workloadReloadAction = null;
            object payload = new String('a', 100);
            if (config.Type.Equals("HYBRID"))
            {
                int queryCount = config.QueryCount;
                workloadReloadAction = workload =>
                {
                    HybridYCSBWorkload hybridWorkload = workload as HybridYCSBWorkload;
                    for (int i = 0; i < queryCount; i++)
                    {
                        hybridWorkload.Keys[i] = generator.NextIntKey();
                        hybridWorkload.Values[i] = payload;
                        hybridWorkload.Queries[i] = generator.NextOperation();
                    }
                };
            }
            else
            {
                workloadReloadAction = workload =>
                {
                    YCSBWorkload ycsbWorkload = workload as YCSBWorkload;
                    ycsbWorkload.Type = generator.NextOperation();
                    ycsbWorkload.Key = generator.NextIntKey();
                    ycsbWorkload.Value = payload;
                };
            }
            return workloadReloadAction;
        }

        private IDataGenerator BuildDataGenerator()
        {
            IDataGenerator generator = null;
            if (config.Dist == Distribution.Uniform)
            {
                generator = new YCSBDataGenerator(this.recordCount, config.ReadPercentage);
            }
            else
            {
                generator = new YCSBDataGenerator(this.recordCount, config.ReadPercentage, Distribution.Zipf, config.Scale);
            }
            return generator;
        }

        private StoredProcedureType GetStoredProcedureType(string name)
        {
            if ("HYBRID".Equals(name))
            {
                return StoredProcedureType.HybridYCSBStordProcedure;
            }
            else
            {
                return StoredProcedureType.YCSBStordProcedure;
            }
        }

        private StoredProcedureWorkload GetStoredProcedureWorkload(string name, int queryCount = 0)
        {
            if ("HYBRID".Equals(name))
            {
                object[] keys = new object[queryCount];
                object[] values = new object[queryCount];
                string[] queries = new string[queryCount];
                return new HybridYCSBWorkload("ycsb_table", keys, values, queries);
            }
            else
            {
                return new YCSBWorkload(name, "ycsb_table", null, new String('a', 100));
            }
        }
    }
}
