namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Linq;
    using System.Collections;

    class TxWorkloadWithTx
    {
        internal string TableId;
        internal string Key;
        internal string Value;
        internal string Type;
        internal Transaction tx;

        public TxWorkloadWithTx(string type, string tableId, string key, string value, Transaction tx)
        {
            this.TableId = tableId;
            this.Key = key;
            this.Value = value;
            this.Type = type;
            this.tx = tx;
        }

        public override string ToString()
        {
            return string.Format("key={0},value={1},type={2},tableId={3}", this.Key, this.Value, this.Type, this.TableId);
        }
    }

    class YCSBBenchmarkTest
    {
        public static readonly String TABLE_ID = "ycsb_table";

        public static readonly long REDIS_DB_INDEX = 7L;

        public static Func<object, object> ACTION = (object obj) =>
        {
            // parse those parameters
            Tuple<TxWorkload, VersionDb> tuple = obj as Tuple<TxWorkload, VersionDb>;
            TxWorkload workload = tuple.Item1;
            VersionDb versionDb = tuple.Item2;
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
			if (tx.Status == TxStatus.Aborted)
			{
				return false;
			}
            // try to commit here
            if (tx.Commit())
            {
                return true;
            }
            //tx.PostProcessingAfterCommit();
			return false;
		};

		public static Func<object, object> ACTION_1 = (object obj) =>
		{
			// parse those parameters
			Tuple<TxWorkload, TransactionExecutor, Transaction> tuple = obj as Tuple<TxWorkload, TransactionExecutor, Transaction>;
			TxWorkload workload = tuple.Item1;
			TransactionExecutor executor = tuple.Item2;
			Transaction tx = tuple.Item3;
			//Transaction tx = executor.CreateTransaction();

			//string readValue = null;
			//switch (workload.Type)
			//{
			//	case "READ":
			//		readValue = (string)tx.Read(workload.TableId, workload.Key);
			//		break;

			//	case "UPDATE":
			//		readValue = (string)tx.Read(workload.TableId, workload.Key);
			//		if (readValue != null)
			//		{
			//			tx.Update(workload.TableId, workload.Key, workload.Value);
			//		}
			//		break;

			//	case "DELETE":
			//		readValue = (string)tx.Read(workload.TableId, workload.Key);
			//		if (readValue != null)
			//		{
			//			tx.Delete(workload.TableId, workload.Key);
			//		}
			//		break;

			//	case "INSERT":
			//		readValue = (string)tx.ReadAndInitialize(workload.TableId, workload.Key);
			//		if (readValue == null)
			//		{
			//			tx.Insert(workload.TableId, workload.Key, workload.Value);
			//		}
			//		break;

			//	default:
			//		break;
			//}
			if (tx.Status == TxStatus.Aborted)
			{
				return false;
			}
            // try to commit here
            if (tx.Commit())
            {
                return true;
            }
            //tx.PostProcessingAfterCommit();
            return false;
		};

		/// <summary>
		/// The number of workers
		/// </summary>
		private int workerCount;

        /// <summary>
        /// The number of tasks per worker
        /// </summary>
        private int taskCountPerWorker;

        /// <summary>
        /// A list of workers
        /// </summary>
        private List<Worker> workers;

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
        /// The version db instance
        /// </summary>
        private VersionDb versionDb;

		private List<TransactionExecutor> executors;
		private List<Transaction> transactions;

        internal int TxThroughput
        {
            get
            {
                double runSeconds = this.RunSeconds;
                int taskCount = this.workerCount * this.taskCountPerWorker;
                return (int)(taskCount / runSeconds);
            }
        }

        //internal double AbortRate
        //{
        //    get
        //    {
        //        return 1 - (COMMITED_TXS * 1.0 / FINISHED_TXS);
        //    }
        //}

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

        public YCSBBenchmarkTest(int workerCount, int taskCountPerWorker, VersionDb versionDb = null)
        {
            this.workerCount = workerCount;
            this.taskCountPerWorker = taskCountPerWorker;
            
            if (versionDb != null)
            {
                this.versionDb = versionDb;
            }

            this.workers = new List<Worker>();
            for (int i = 0; i < this.workerCount; i++)
            {
                this.workers.Add(new Worker(i+1, taskCountPerWorker));
            }

			this.executors = new List<TransactionExecutor>();
			this.transactions = new List<Transaction>();
			for (int i = 0; i < this.workerCount; i++)
			{
				this.executors.Add(new TransactionExecutor(this.versionDb, null, null, 0, i));
				this.transactions.Add(new Transaction(null, this.versionDb, 10));
			}
        }

        /// <summary>
        /// split function "Setup" into `LoadData` and `LoadWorkloads` 
        /// </summary>
        /// <param name="dataFile"></param>
        internal void LoadData(string dataFile, int loadCountMax = -1)
        {
            // step1: clear the database
            this.versionDb.Clear();
            Console.WriteLine("Cleared the database");

            // step2: create version table
            versionDb.CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);
            Console.WriteLine("Created version table {0}", TABLE_ID);

            // step3: load data
            using (StreamReader reader = new StreamReader(dataFile))
            {
                string line;
                int count = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseCommandFormat(line);
                    TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                    count++;

                    ACTION(Tuple.Create(workload, this.versionDb));
                    if (count % 1000 == 0)
                    {
                        Console.WriteLine("Loaded {0} records", count);
                    }
                    if (count == loadCountMax)
                    {
                        break;
                    }
                }
                Console.WriteLine("Load records successfully, {0} records in total", count);
            }
        }


        void batchInsert(int s, int e, VersionDb vdb, List<string> lines)
        {
            //Console.WriteLine("s={0}, e={1}", s, e);
            Transaction tx = new Transaction(null, vdb);
            string readValue = null;
            for (int i = s; i < e; i++)
            {
                //Console.WriteLine("i={0}, cnt={1}", i, lines.Count);
                string[] fields = this.ParseCommandFormat(lines[i]);
                //readValue = (string)tx.ReadAndInitialize(TABLE_ID, fields[2]);
                readValue = (string)tx.ReadAndInitialize(TABLE_ID, i);
                if (readValue == null)
                {
                    //tx.Insert(TABLE_ID, fields[2], fields[3]);
                    tx.Insert(TABLE_ID, i, fields[3]);
                }
            }
            tx.Commit();
        }
        
        void loadWithOneThread(int tid, int s, int e, List<string> lines, List<VersionDb> vdbList, int[] finishedCnt)
        {
            string line;
            for (int i = s; i < e; i++)
            {
                line = lines[i];
                string[] fields = this.ParseCommandFormat(line);
                TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                ACTION(Tuple.Create(workload, vdbList[tid]));
                finishedCnt[tid] += 1;
            }
        }

        void loadWithOneThreadBatch(int tid, int s, int e, List<VersionDb> vdbList, int[] finishedCnt, List<string> lines)
        {
            //Console.WriteLine("tid={0}, s={1}, e={2}", tid, s, e);
            int batchSize = 50;
            string line;
            for (int i = s; i < e; i += batchSize)
            {
                int end = i + batchSize;
                if (end > e)
                {
                    end = e;
                }
                batchInsert(i, end, vdbList[tid], lines);

                finishedCnt[tid] += (end - i);
                if (finishedCnt[tid] % 100 == 0)
                {
                    //Console.WriteLine("Load {0}", finishedCnt[tid]);
                }
            }
        }

        internal void LoadDataWithMultiThreads(string dataFile, List<VersionDb> vdbList, int threadCount = 8)
        {
            long beginTicks = DateTime.Now.Ticks;

            if (vdbList.Count < threadCount)
            {
                //throw new Exception("version db instance not enough");
            }

            // step1: clear the database
            vdbList[0].Clear();
            Console.WriteLine("Cleared the database");

            // step2: create version table
            vdbList[0].CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);
            Console.WriteLine("Created version table {0}", TABLE_ID);

            // step 3: load data with multi threads
            List<string> lines = new List<string>();
            using (StreamReader sr = new StreamReader(dataFile))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    lines.Add(line);
                }
            }
            Console.WriteLine("To Load {0} with {1} threads", lines.Count, threadCount);

            int[] finishedCnt = new int[threadCount];
            for (int i=0; i<threadCount; i++)
            {
                finishedCnt[i] = 0;
            }

            //void batchInsert(int s, int e, VersionDb vdb)
            //{
            //    //Console.WriteLine("s={0}, e={1}", s, e);
            //    Transaction tx = new Transaction(null, vdb);
            //    string readValue = null;
            //    for (int i = s; i < e; i++)
            //    {
            //        //Console.WriteLine("i={0}, cnt={1}", i, lines.Count);
            //        string[] fields = this.ParseCommandFormat(lines[i]);
            //        //readValue = (string)tx.ReadAndInitialize(TABLE_ID, fields[2]);
            //        readValue = (string)tx.ReadAndInitialize(TABLE_ID, i);
            //        if (readValue == null)
            //        {
            //            //tx.Insert(TABLE_ID, fields[2], fields[3]);
            //            tx.Insert(TABLE_ID, i, fields[3]);
            //        }
            //    }
            //    tx.Commit();
            //}

            //void loadWithOneThread(int tid, int s, int e)
            //{
            //    string line;
            //    for (int i = s; i < e; i++)
            //    {
            //        line = lines[i];
            //        string[] fields = this.ParseCommandFormat(line);
            //        TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
            //        ACTION(Tuple.Create(workload, vdbList[tid]));
            //        finishedCnt[tid] += 1;
            //    }
            //}
            //void loadWithOneThreadBatch(int tid, int s, int e)
            //{
            //    //Console.WriteLine("tid={0}, s={1}, e={2}", tid, s, e);
            //    int batchSize = 50;
            //    string line;
            //    for (int i = s; i < e; i += batchSize)
            //    {
            //        int end = i + batchSize;
            //        if (end > e)
            //        {
            //            end = e;
            //        }
            //        batchInsert(i, end, vdbList[tid]);

            //        finishedCnt[tid] += (end - i);
            //        if (finishedCnt[tid] % 100 == 0)
            //        {
            //            //Console.WriteLine("Load {0}", finishedCnt[tid]);
            //        }
            //    }
            //}

            //loadWithOneThreadBatch(0, 0, lines.Count);

            int cntPerThread = lines.Count / threadCount;
            List<Thread> threads = new List<Thread>();
            for (int i = 0; i < threadCount; i++)
            {
                //Console.WriteLine("i=" + i);
                Thread t;
                if (i == threadCount - 1)
                {
                    t = new Thread(() => loadWithOneThreadBatch(i, i * cntPerThread, lines.Count, vdbList, finishedCnt, lines));
                }
                else
                {
                    t = new Thread(() => loadWithOneThreadBatch(i, i * cntPerThread, (i + 1) * cntPerThread, vdbList, finishedCnt, lines));
                }
                threads.Add(t);
                t.Start();
                Thread.Sleep(1000);
            }

            while (true)
            {
                Thread.Sleep(1000); // 1s
                int doneTotal = 0;
                for (int i = 0; i < threadCount; i++)
                {
                    doneTotal += finishedCnt[i];
                }
                Console.WriteLine("Load {0}", doneTotal);
                if (doneTotal == lines.Count)
                {
                    foreach (Thread t in threads)
                    {
                        t.Join();
                    }
                    break;
                }
            }
            long endTicks = DateTime.Now.Ticks;
            Console.WriteLine("Load Finished, total {0}, Elapsed {1} seconds", lines.Count, (endTicks-beginTicks)*1.0/10000000);
        }

        internal void Reset(int workerCount, int taskCountPerWorker, List<VersionDb> vdbList)
        {
            this.workerCount = workerCount;
            this.taskCountPerWorker = taskCountPerWorker;

            // clear tx_table
            vdbList[0].ClearTxTable();

            this.workers.Clear();
            for (int i = 0; i < this.workerCount; i++)
            {
                this.workers.Add(new Worker(i + 1, taskCountPerWorker));
            }

            this.executors.Clear();
            this.transactions.Clear();
            for (int i = 0; i < this.workerCount; i++)
            {
                this.executors.Add(new TransactionExecutor(vdbList[i], null, null, 0, i));
                this.transactions.Add(new Transaction(null, vdbList[i], 10));
            }
        }

        internal void Reset(int workerCount, int taskCountPerWorker)
        {
            this.workerCount = workerCount;
            this.taskCountPerWorker = taskCountPerWorker;
            
            this.workers.Clear();
            for (int i = 0; i < this.workerCount; i++)
            {
                this.workers.Add(new Worker(i + 1, taskCountPerWorker));
            }

            this.executors.Clear();
            this.transactions.Clear();
            for (int i = 0; i < this.workerCount; i++)
            {
                this.executors.Add(new TransactionExecutor(this.versionDb, null, null, 0, i));
                this.transactions.Add(new Transaction(null, this.versionDb, 10));
            }
        }

        internal void LoadWorkloads(string opFile)
        {
            // step 4: fill workers' queue
            using (StreamReader reader = new StreamReader(opFile))
            {
                string line;
                for (int worker_index = 0; worker_index < this.workerCount; worker_index++)
                {
                    for (int i = 0; i < this.taskCountPerWorker; i++)
                    {
                        line = reader.ReadLine();
                        string[] fields = this.ParseCommandFormat(line);
                        TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                        this.workers[worker_index].EnqueueTxTask(new TxTask(ACTION, Tuple.Create(workload, this.versionDb)));
                    }
                }
            }
        }

        internal void LoadWorkloads(string opFile, List<VersionDb> vdbList)
        {
            if (vdbList.Count < this.workerCount)
            {
                throw new Exception("not enough version db");
            }

            // step 4: fill workers' queue
            using (StreamReader reader = new StreamReader(opFile))
            {
                string line;
                for (int worker_index = 0; worker_index < this.workerCount; worker_index++)
                {
                    for (int i = 0; i < this.taskCountPerWorker; i++)
                    {
                        line = reader.ReadLine();
                        string[] fields = this.ParseCommandFormat(line);
                        TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                        this.workers[worker_index].EnqueueTxTask(new TxTask(ACTION, Tuple.Create(workload, vdbList[worker_index])));
                    }
                }
            }
        }




        internal void Setup(string dataFile, string operationFile)
        {
            // step1: clear the database
            this.versionDb.Clear();
            Console.WriteLine("Cleared the database");

            // step2: create version table
            versionDb.CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);
            Console.WriteLine("Created version table {0}", TABLE_ID);

            // step3: load data
            using (StreamReader reader = new StreamReader(dataFile))
            {
                string line;
                int count = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseCommandFormat(line);
                    TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                    count++;

                    ACTION(Tuple.Create(workload, this.versionDb));
                    if (count % 10000 == 0)
                    {
                        Console.WriteLine("Loaded {0} records", count);
                    }
                    if (count == 2000000)
                    //if (count == 5000)
                    {
                        break;
                    }
                }
                Console.WriteLine("Load records successfully, {0} records in total", count);
            }

            //this.versionDb.ClearTxTable();

            //preload to make txtable and txqueue full.
            //for (int worker_index = 0; worker_index < this.workerCount; worker_index++)
            //{
            //    for (int i = 0; i < TxRange.range; i++)
            //    {
            //        this.versionDb.InsertNewTx(i + worker_index * TxRange.range);
            //        this.executors[worker_index].GarbageQueueTxId.Enqueue(i + worker_index * TxRange.range);
            //        this.executors[worker_index].GarbageQueueFinishTime.Enqueue(0);
            //    }
            //}

            // step 4: fill workers' queue
            using (StreamReader reader = new StreamReader(operationFile))
            {
                string line;
                for (int worker_index = 0; worker_index < this.workerCount; worker_index++)
                {
                    for (int i = 0; i < this.taskCountPerWorker; i++)
                    {
                        line = reader.ReadLine();
                        string[] fields = this.ParseCommandFormat(line);
                        TxWorkload workload = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
						this.workers[worker_index].EnqueueTxTask(new TxTask(ACTION, Tuple.Create(workload, this.versionDb)));
						//this.workers[worker_index].EnqueueTxTask(new TxTask(ACTION_1, Tuple.Create(workload, this.executors[worker_index], this.transactions[worker_index])));
                    }
                }
            }
        }

        internal void Run()
        {
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.workerCount * this.taskCountPerWorker), this.workerCount);
            Console.WriteLine("Running......");

            // ONLY FOR REDIS VERSION DB
            long commandCountBeforeRun = long.MaxValue;
            if (this.versionDb is RedisVersionDb)
            {
                commandCountBeforeRun = this.GetCurrentCommandCount();
            }

            this.testBeginTicks = DateTime.Now.Ticks;
            List<Thread> threadList = new List<Thread>();

            foreach (Worker worker in this.workers)
            {
                Thread thread = new Thread(new ThreadStart(worker.Run));
                threadList.Add(thread);
                thread.Start();
            }

            foreach (Thread thread in threadList)
            {
                thread.Join();
            }
            this.testEndTicks = DateTime.Now.Ticks;

            if (this.versionDb is RedisVersionDb)
            {
                long commandCountAfterRun = this.GetCurrentCommandCount();
                this.commandCount = commandCountAfterRun - commandCountBeforeRun;
            }

            Console.WriteLine("Finished all tasks");
        }

        internal void rerun(int workerCount, int workloadCountPerWorker, string opFile, List<VersionDb> vdbList)
        {
            this.Reset(workerCount, workloadCountPerWorker, vdbList);
            this.LoadWorkloads(opFile, vdbList);
            this.Run();
            this.Stats();
        }


        internal void Stats()
        {
            int taskCount = this.workerCount * this.taskCountPerWorker;
            Console.WriteLine("\nFinshed {0} requests in {1} seconds", taskCount, this.RunSeconds);
            Console.WriteLine("Transaction Throughput: {0} tx/second", this.TxThroughput);
			Console.WriteLine("-----------------------------------------------------------------");

			int totalTxs = 0, abortedTxs = 0;
			for (int worker_index = 0; worker_index < this.workerCount; worker_index++)
			{
                totalTxs += this.workers[worker_index].FinishedTxs;
                abortedTxs += this.workers[worker_index].AbortedTxs;
				//Console.WriteLine("Worker Index: {0}", worker_index);
				////Console.WriteLine("Throughput: {0} tx/second", (this.taskCountPerWorker-1000000)/this.workers[worker_index].RunSeconds);
				//Console.WriteLine("Throughput: {0} tx/second", (this.taskCountPerWorker) / this.workers[worker_index].RunSeconds);
				//Console.WriteLine("RecycleCount: {0}", this.executors[worker_index].RecycleCount);
    //            Console.WriteLine("InsertNewTxCount: {0}", this.executors[worker_index].InsertNewTxCount);
    //            Console.WriteLine("Run Time: {0} second", this.workers[worker_index].RunSeconds);
			}


			Console.WriteLine("-----------------------------------------------------------------");
			Console.WriteLine("\nFinshed {0} txs, Aborted {1} txs", totalTxs, abortedTxs);
            Console.WriteLine("Transaction AbortRate: {0}%", (abortedTxs*1.0/totalTxs) * 100);

            if (this.versionDb is RedisVersionDb)
            {
                Console.WriteLine("\nFinshed {0} commands in {1} seconds", this.commandCount, this.RunSeconds);
                Console.WriteLine("Redis Throughput: {0} cmd/second", this.RedisThroughput);
            }
            
            Console.WriteLine();
        }

        private string[] ParseCommandFormat(string line)
        {
            string[] fields = line.Split(' ');
            string value = null;
            int fieldsOffset = fields[0].Length + fields[1].Length + fields[2].Length + 3 + 9;
            int fieldsEnd = line.Length - 2;

            if (fieldsOffset < fieldsEnd)
            {
                value = line.Substring(fieldsOffset, fieldsEnd-fieldsOffset+1);
            }

            return new string[] {
                fields[0], fields[1], fields[2], value
            };
        }

        /// <summary>
        /// ONLY FOR REDIS VERSION DB
        /// 
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
            RedisVersionDb redisVersionDb = this.versionDb as RedisVersionDb;
            if (redisVersionDb == null)
            {
                return 0;
            }

            RedisClientManager clientManager = redisVersionDb.RedisManager;
            clientManager.Dispose();
            long commandCount = 0;
            for (int i = 0; i < clientManager.RedisInstanceCount; i++)
            {
                using (RedisClient redisClient = clientManager.GetLastestClient(0, 0))
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
