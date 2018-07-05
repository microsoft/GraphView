namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    struct RedisWorkload
    {
        internal string hashId;
        internal byte[] key;
        internal byte[] value;
        internal RedisWorkloadType type;
    }

    enum RedisWorkloadType
    {
        HSet,
        HSetNX,
        HMSet,
        HGet,
        HMGet,
        HGetAll,
    }

    class RedisBenchmarkTest
    {
        public static readonly long REDIS_DB_INDEX = 3L;

        public static RedisVersionDb REDIS_VERSION_DB = RedisVersionDb.Instance();

        public static Func<object, object> ACTION = (object obj) =>
        {
            Tuple<RedisWorkload, int> tuple = (Tuple<RedisWorkload, int>)obj;
            RedisWorkload workload = (RedisWorkload)tuple.Item1;
            int partition = tuple.Item2;

            using (RedisClient client = REDIS_VERSION_DB.RedisManager.GetClient(REDIS_DB_INDEX, partition))
            {
                switch (workload.type)
                {
                    case RedisWorkloadType.HSet:
                        client.HSet(workload.hashId, workload.key, workload.value);
                        break;

                    case RedisWorkloadType.HSetNX:
                        client.HSetNX(workload.hashId, workload.key, workload.value);
                        break;

                    case RedisWorkloadType.HMSet:
                        client.HMSet(workload.hashId, new byte[][] { workload.key }, new byte[][] { workload.value });
                        break;

                    case RedisWorkloadType.HGet:
                        client.HGet(workload.hashId, workload.key);
                        break;

                    case RedisWorkloadType.HGetAll:
                        client.HGetAll(workload.hashId);
                        break;

                    case RedisWorkloadType.HMGet:
                        client.HMGet(workload.hashId, new byte[][] { workload.key });
                        break;
                }
            }
            return true;
        };

        public static Func<object, object> PIPELINE_ACTION = (object obj) =>
        {
            Tuple<RedisWorkload[], int> tuple = (Tuple<RedisWorkload[], int>)obj;
            RedisWorkload[] workloads = tuple.Item1;
            int partition = tuple.Item2;

            using (RedisClient client = REDIS_VERSION_DB.RedisManager.GetClient(REDIS_DB_INDEX, partition))
            {
                using (IRedisPipeline pipe = client.CreatePipeline())
                {
                    foreach (RedisWorkload workload in workloads)
                    {
                        switch (workload.type)
                        {
                            case RedisWorkloadType.HSet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(workload.hashId, workload.key, workload.value));
                                break;

                            case RedisWorkloadType.HSetNX:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HSetNX(workload.hashId, workload.key, workload.value));
                                break;

                            case RedisWorkloadType.HMSet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HMSet(workload.hashId, new byte[][] { workload.key }, new byte[][] { workload.value }));
                                break;

                            case RedisWorkloadType.HGet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HGet(workload.hashId, workload.key));
                                break;

                            case RedisWorkloadType.HGetAll:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HGetAll(workload.hashId));
                                break;

                            case RedisWorkloadType.HMGet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HMGet(workload.hashId, new byte[][] { workload.key }));
                                break;
                        }
                    }
                    pipe.Flush();
                }
            }
            return true;
        };

        /// <summary>
        /// The number of running workers
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
        /// Whether it's in pipeline mode
        /// </summary>
        private bool pipelineMode;

        /// <summary>
        /// If it's in the pipeline mode, the pipeline size
        /// </summary>
        private int pipelineSize;

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

        public RedisBenchmarkTest(int workerCount, int taskCount, bool pipelineMode = false, int pipelineSize = 100)
        {
            this.workerCount = workerCount;
            this.taskCountPerWorker = taskCount;
            this.pipelineMode = pipelineMode;
            this.pipelineSize = pipelineSize;
            this.workers = new List<Worker>();

            for (int i = 0; i < workerCount; i++)
            {
                this.workers.Add(new Worker(i + 1, Math.Max(taskCount, Worker.DEFAULT_QUEUE_SIZE)));
            }
        }

        internal void Setup()
        {
            RedisClientManager manager = REDIS_VERSION_DB.RedisManager;
            using (RedisClient client = manager.GetClient(REDIS_DB_INDEX, 0))
            {
                client.FlushDb();
            }
            Console.WriteLine("Flushed the database");

            int count = 0, redisInstances = REDIS_VERSION_DB.RedisManager.RedisInstanceCount;
            foreach (Worker worker in workers)
            {
                int partition = (worker.WorkerId - 1) % REDIS_VERSION_DB.RedisManager.RedisInstanceCount;

                // non-pipeline mode
                if (!this.pipelineMode)
                {
                    for (int i = 0; i < this.taskCountPerWorker; i++)
                    {
                        worker.EnqueueTxTask(new TxTask(ACTION, Tuple.Create(mockRedisWorkload(), partition)));
                    }
                }
                // pipeline mode
                else
                {
                    int batchs = this.taskCountPerWorker / this.pipelineSize;
                    RedisWorkload[] workloads = new RedisWorkload[this.pipelineSize];
                    for (int i = 0; i < batchs; i++)
                    {
                        for (int j = 0; j < this.pipelineSize; j++)
                        {
                            workloads[j] = mockRedisWorkload(3);
                        }
                        worker.EnqueueTxTask(new TxTask(PIPELINE_ACTION, Tuple.Create(workloads, partition)));
                    }
                }
                Console.WriteLine("Setup {0} workers", ++count);
            }

            for (int redisIndex = 0; redisIndex < redisInstances; redisIndex++)
            {
                Console.WriteLine("Redis Instance: {0} has {1} workers", redisIndex, this.workerCount/redisInstances);
            }
            Console.WriteLine("Filled the workers' queue");
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

            Console.WriteLine("Threads Count: {0}", threadList.Count);

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
            Console.WriteLine("\nSent {0} requests in {1} seconds", taskCount, this.RunSeconds);
            Console.WriteLine("\nRedis Finshed {0} commands in {1} seconds", this.commandCount, this.RunSeconds);
            Console.WriteLine("Redis Throughput: {0} cmd/second", this.RedisThroughput);

            Console.WriteLine();
        }

        private static RedisWorkload mockRedisWorkload(int type = -1)
        { 
            RedisWorkload workload = new RedisWorkload();
            workload.hashId = RandomString(10);
            workload.key = BitConverter.GetBytes(RAND.Next(0, int.MaxValue));
            workload.type = (RedisWorkloadType)(type == -1 ? RAND.Next(0, 6) : type);
            if (workload.type == RedisWorkloadType.HGet ||
                workload.type == RedisWorkloadType.HGetAll ||
                workload.type == RedisWorkloadType.HMGet)
            {
                workload.value = null;
            }
            else
            {
                workload.value = RandomBytes(32);
            }
            return workload;
        }

        private long GetCurrentCommandCount()
        {
            RedisClientManager clientManager = REDIS_VERSION_DB.RedisManager;
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

        /// Helper Functions to test benchmarks
        /// 
        public static Random RAND = new Random();

        public static string CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public static Func<int, string> RandomString = (int length) =>
        {
            return new string(Enumerable.Repeat(CHARS, length)
                .Select(s => s[RAND.Next(s.Length)]).ToArray());
        };

        public static Func<int, byte[]> RandomBytes = (int length) =>
        {
            byte[] value = new byte[length];
            RAND.NextBytes(value);
            return value;
        };

    }
}
