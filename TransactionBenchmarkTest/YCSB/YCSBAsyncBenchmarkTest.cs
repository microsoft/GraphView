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

        public static Func<object, object> ACTION = (object op) =>
        {
            TxWorkload oper = op as TxWorkload;
            Transaction tx = new Transaction(null, RedisVersionDb.Instance);
            string readValue = null;                          
            try
            {
                switch (oper.Type)
                {
                    case "READ":
                        readValue = (string)tx.Read(oper.TableId, oper.Key);
                        break;

                    case "UPDATE":
                        readValue = (string)tx.Read(oper.TableId, oper.Key);
                        if (readValue != null)
                        {
                            tx.Update(oper.TableId, oper.Key, oper.Value);
                        }
                        break;

                    case "DELETE":
                        readValue = (string)tx.Read(oper.TableId, oper.Key);
                        if (readValue != null)
                        {
                            tx.Delete(oper.TableId, oper.Key);
                        }
                        break;

                    case "INSERT":
                        readValue = (string)tx.ReadAndInitialize(oper.TableId, oper.Key);
                        if (readValue == null)
                        {
                            tx.Insert(oper.TableId, oper.Key, oper.Value);
                        }
                        break;

                    default:
                        break;
                }
                tx.Commit();
                // commited here
                return true;
            }
            catch (TransactionException e)
            {
                // aborted here
                return false;
            }
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
        /// The redis version db instance
        /// </summary>
        private RedisVersionDb redisVersionDb;

        private List<List<Tuple<string, int>>> partitionedInstances;

        internal int TxThroughput
        {
            get
            {
                int txCount = this.txCountPerExecutor * this.executorCount;
                double runSeconds = this.RunSeconds;
                return (int)(txCount / runSeconds);
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

        public YCSBAsyncBenchmarkTest(int executorCount, int txCountPerExecutor, List<List<Tuple<string, int>>> instances = null)
        {
            this.redisVersionDb = RedisVersionDb.Instance;

            this.executorCount = executorCount;
            this.txCountPerExecutor = txCountPerExecutor;

            this.executorList = new List<TransactionExecutor>();

            if (instances == null || instances.Count > executorCount)
            {
                throw new ArgumentException("instances mustn't be null and the size should be smaller or equal to executorCount");
            }
            this.partitionedInstances = instances;
        }

        internal void Setup(string dataFile, string operationFile)
        {
            // step1: flush the database
            RedisClientManager manager = this.redisVersionDb.RedisManager;

            using (RedisClient client = manager.GetClient(REDIS_DB_INDEX, 0))
            {
                client.FlushAll();
            }
            Console.WriteLine("Flushed the database");

            // step2: create version table
            this.redisVersionDb.CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);

            // step3: load data
            using (StreamReader reader = new StreamReader(dataFile))
            {
                string line;
                int count = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseCommandFormat(line);
                    TxWorkload operation = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                    count++;

                    ACTION(operation);
                    if (count % 10000 == 0)
                    {
                        Console.WriteLine("Loaded {0} records", count);
                        break;
                    }
                }
                Console.WriteLine("Load records successfully, {0} records in total", count);
            }

            // step 4: fill workers' queue
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

                    List<Tuple<string, int>> executorInstances = instanceIndex >= this.partitionedInstances.Count ? null :
                        this.partitionedInstances[instanceIndex++];
                    this.executorList.Add(new TransactionExecutor(this.redisVersionDb, null, reqQueue, executorInstances));
                }
            }
        }

        internal void Run()
        {
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.executorCount * this.txCountPerExecutor), this.executorCount);
            Console.WriteLine("Running......");

            long commandCountBeforeRun = this.GetCurrentCommandCount();

            this.testBeginTicks = DateTime.Now.Ticks;
            List<Thread> threadList = new List<Thread>();

            foreach (TransactionExecutor executor in this.executorList)
            {
                Thread thread = new Thread(new ThreadStart(executor.Execute));
                threadList.Add(thread);
                thread.Start();
            }

            while (true)
            {
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

            this.testEndTicks = DateTime.Now.Ticks;

            long commandCountAfterRun = this.GetCurrentCommandCount();
            this.commandCount = commandCountAfterRun - commandCountBeforeRun;

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

            Console.WriteLine("\nFinshed {0} commands in {1} seconds", this.commandCount, this.RunSeconds);
            Console.WriteLine("Redis Throughput: {0} cmd/second", this.RedisThroughput);

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
