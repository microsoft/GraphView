namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;

    class TxWorkload
    {
        internal string TableId;
        internal string Key;
        internal string Value;
        internal string Type;

        public TxWorkload(string type, string tableId, string key, string value)
        {
            this.TableId = tableId;
            this.Key = key;
            this.Value = value;
            this.Type = type;
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

        private RedisVersionDb versionDb;

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

        public YCSBBenchmarkTest(int workerCount, int taskCountPerWorker)
        {
            this.workerCount = workerCount;
            this.taskCountPerWorker = taskCountPerWorker;
            this.workers = new List<Worker>();

            for (int i = 0; i < this.workerCount; i++)
            {
                this.workers.Add(new Worker(i+1, taskCountPerWorker));
            }
        }

        internal void Setup(string dataFile, string operationFile)
        {
            // step1: flush the database
            this.versionDb = RedisVersionDb.Instance;
            RedisClientManager manager = this.versionDb.RedisManager;

            using (RedisClient client = manager.GetClient(REDIS_DB_INDEX, 0))
            {
                client.FlushAll();
            }
            Console.WriteLine("Flushed the database");

            // step2: create version table
            versionDb.CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);

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
                    if (count % 5000 == 0)
                    {
                        Console.WriteLine("Loaded {0} records", count);
                    }
                }
                Console.WriteLine("Load records successfully, {0} records in total", count);
            }

            // step 4: fill workers' queue
            using (StreamReader reader = new StreamReader(operationFile))
            {
                string line;
                foreach (Worker worker in this.workers)
                {
                    for (int i = 0; i < this.taskCountPerWorker; i++)
                    {
                        line = reader.ReadLine();
                        string[] fields = this.ParseCommandFormat(line);
                        TxWorkload op = new TxWorkload(fields[0], TABLE_ID, fields[2], fields[3]);
                        worker.EnqueueTxTask(new TxTask(ACTION, op));
                    }
                }
            }
        }

        internal void Run()
        {
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.workerCount * this.taskCountPerWorker), this.workerCount);
            Console.WriteLine("Running......");

            long commandCountBeforeRun = this.GetCurrentCommandCount();

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

            long commandCountAfterRun = this.GetCurrentCommandCount();
            this.commandCount = commandCountAfterRun - commandCountBeforeRun;

            Console.WriteLine("Finished all tasks");
        }

        internal void Stats()
        {
            int taskCount = this.workerCount * this.taskCountPerWorker;
            Console.WriteLine("\nFinshed {0} requests in {1} seconds", taskCount, this.RunSeconds);
            Console.WriteLine("Transaction Throughput: {0} tx/second", this.TxThroughput);

            int totalTxs = 0, abortedTxs = 0;
            foreach (Worker worker in this.workers)
            {
                totalTxs += worker.FinishedTxs;
                abortedTxs += worker.AbortedTxs;
            }
            Console.WriteLine("\nFinshed {0} txs, Commited {1} txs", totalTxs, abortedTxs);
            Console.WriteLine("Transaction AbortRate: {0}%", (abortedTxs*1.0/totalTxs) * 100);

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
                value = line.Substring(fieldsOffset, fieldsEnd-fieldsOffset+1);
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
            RedisClientManager clientManager = this.versionDb.RedisManager;
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
