namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    class ThroughputBenchmarkTest : IDisposable
    {
        public static int FINISHED_TXS = 0;

        public static int COMMITED_TXS = 0;

        public static Func<object, object> ACTION = (object op) =>
        {
            Operation oper = op as Operation;
            Transaction tx = new Transaction(null, RedisVersionDb.Instance);
            string readValue = null;

            try
            {
                switch (oper.Operator)
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

                FINISHED_TXS += 1;
                if (tx.Status == TxStatus.Committed)
                {
                    COMMITED_TXS += 1;
                }
            }
            catch (TransactionException e)
            {

            }
            return null;
        };

        public static readonly String TABLE_ID = "usertable_concurrent";

        public static readonly long REDIS_DB_INDEX = 7L;

        private int workerCount;

        private List<Worker> workerList;

        public ThroughputBenchmarkTest(int workerCount)
        {
            this.workerCount = workerCount;
            this.workerList = new List<Worker>();

            for (int i = 0; i < this.workerCount; i++)
            {
                this.workerList.Add(new Worker(i+1));
            }
        }

        internal void SetupTest(string dataFile)
        {
            // flush the database
            RedisClientManager manager = RedisClientManager.Instance;
            RedisVersionDb versionDb = RedisVersionDb.Instance;
            using (RedisClient client = manager.GetClient(REDIS_DB_INDEX, 0))
            {
                client.FlushAll();
                Console.WriteLine("Flushed the database");
            }

            // create version table
            versionDb.CreateVersionTable(TABLE_ID, REDIS_DB_INDEX);

            // load data
            using (StreamReader reader = new StreamReader(dataFile))
            {
                string line;
                int count = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseCommandFormat(line);
                    Operation operation = new Operation(fields[0], TABLE_ID, fields[2], fields[3]);
                    count++;

                    ACTION(operation);
                    if (count % 1000 == 0)
                    {
                        Console.WriteLine("Loaded {0} records", count);
                    }
                }
            }

            //Console.WriteLine("TOTAL_FLUSH: {0}, TIME_FLUSH: {1}, COMMANDS_FLUSH: {2}", RedisConnectionPool.TOTAL_FLUSH, RedisConnectionPool.TIME_FLUSH, RedisConnectionPool.COMMANDS_FLUSHED);
            //Console.WriteLine("FINISHED_TXS: {0}, COMMITED_TXS: {1}", FINISHED_TXS, COMMITED_TXS);
            //Console.WriteLine("Loaded all records");
        }

        internal void FillWorkersQueue(string optrFilePrefix)
        {
            for (int i = 0; i < this.workerCount; i++)
            {
                Worker worker = this.workerList[i];
                string resourceName = String.Format("{0}{1}.in", optrFilePrefix, i+1);
                using (StreamReader reader = new StreamReader(resourceName))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] fields = this.ParseCommandFormat(line);
                        Operation op = new Operation(fields[0], ConcurrencyRunner.TABLE_ID, fields[2], fields[3]);
                        worker.EnqueueTxTask(new Task<object>(ACTION, op));
                    }
                }
            }

            Console.WriteLine("Filled all workers' queue");
        }

        internal void Run()
        {
            this.StartWorkers();
        }

        internal void Conclude()
        {
            //bool allFinished = false;
            //while (!allFinished)
            //{
            //    int i = 0;
            //    for (; i < this.workerCount; i++)
            //    {
            //        if (this.workerList[i].ExecutionTime == -1)
            //        {
            //            break;
            //        }
            //    }
            //    if (i >= this.workerCount)
            //    {
            //        allFinished = true;
            //    }
            //}

            //int throughput = 0;
            //foreach (Worker worker in this.workerList)
            //{
            //    throughput += worker.Throughput;
            //}

            //Console.WriteLine("Throughput: {0} txs/second", throughput);
        }

        private void StartWorkers()
        {
            foreach (Worker worker in this.workerList)
            {
                worker.Active = true;
                Thread thread = new Thread(new ThreadStart(worker.Monitor));
                thread.Start();
            }
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

        public void Dispose()
        {
            foreach (Worker worker in this.workerList)
            {
                worker.Dispose();
            }
        }
    }
}
