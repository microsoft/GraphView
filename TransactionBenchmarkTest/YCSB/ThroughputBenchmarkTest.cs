namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;

    class TxOperation
    {
        internal string TableId;
        internal string Key;
        internal string Value;
        internal string Type;

        public TxOperation(string tableId, string key, string value, string type)
        {
            this.TableId = tableId;
            this.Key = key;
            this.Value = value;
            this.Type = type;
        }
    }

    class YCSBBenchmarkTest
    {
        public static int FINISHED_TXS = 0;

        public static int COMMITED_TXS = 0;

        public static readonly String TABLE_ID = "ycsb_table";

        public static readonly long REDIS_DB_INDEX = 7L;

        public static Func<object, object> ACTION = (object op) =>
        {
            TxOperation oper = op as TxOperation;
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

                YCSBBenchmarkTest.FINISHED_TXS += 1;
                if (tx.Status == TxStatus.Committed)
                {
                    YCSBBenchmarkTest.COMMITED_TXS += 1;
                }
            }
            catch (TransactionException e)
            {

            }
            return null;
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

        internal int Throughput
        {
            get
            {
                double runSeconds = ((this.testEndTicks - this.testBeginTicks) * 1.0) / 10000000;
                int taskCount = this.workerCount * this.taskCountPerWorker;
                Console.WriteLine("Finshed {0} requests in {1} seconds", taskCount, runSeconds);
                return (int)(taskCount / runSeconds);
            }
        }


        public YCSBBenchmarkTest(int workerCount, int taskCountPerWorker)
        {
            this.workerCount = workerCount;
            this.taskCountPerWorker = taskCountPerWorker;
            this.workers = new List<Worker>();

            for (int i = 0; i < this.workerCount; i++)
            {
                this.workers.Add(new Worker(i+1));
            }
        }

        internal void Setup(string dataFile, string operationFile)
        {
            // step1: flush the database
            RedisClientManager manager = RedisClientManager.Instance;
            RedisVersionDb versionDb = RedisVersionDb.Instance;

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
                    TxOperation operation = new TxOperation(fields[0], TABLE_ID, fields[2], fields[3]);
                    count++;

                    ACTION(operation);
                    if (count % 1000 == 0)
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
                        TxOperation op = new TxOperation(fields[0], TABLE_ID, fields[2], fields[3]);
                        worker.EnqueueTxTask(new TxTask(ACTION, op));
                    }
                }
            }
        }

        internal void Run()
        {
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.workerCount * this.taskCountPerWorker), this.workerCount);
            Console.WriteLine("Running......");

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

            Console.WriteLine("Finished all tasks");
        }

        private string[] ParseCommandFormat(string line)
        {
            string[] fields = line.Split(' ');
            string value = null;
            if (fields[4].Length > 6)
            {
                value = fields[4].Substring(7, fields[4].Length - 7);
            }

            return new string[] {
                fields[0], fields[1], fields[2], value
            };
        }
    }
}
