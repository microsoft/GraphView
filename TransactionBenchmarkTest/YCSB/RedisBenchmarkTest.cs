namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    class RedisWorkload
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
        EvalSha,
    }

    class RedisBenchmarkTest
    {
        public static readonly long REDIS_DB_INDEX = 3L;

        public static Func<object, object> ACTION = (object obj) =>
        {
            RedisWorkload cmd = (RedisWorkload)obj;
            using (RedisClient client = RedisClientManager.Instance.GetClient(REDIS_DB_INDEX, 0))
            {
                switch (cmd.type)
                {
                    case RedisWorkloadType.HSet:
                        client.HSet(cmd.hashId, cmd.key, cmd.value);
                        break;

                    case RedisWorkloadType.HSetNX:
                        client.HSetNX(cmd.hashId, cmd.key, cmd.value);
                        break;

                    case RedisWorkloadType.HMSet:
                        client.HMSet(cmd.hashId, new byte[][] { cmd.key }, new byte[][] { cmd.value });
                        break;

                    case RedisWorkloadType.HGet:
                        client.HGet(cmd.hashId, cmd.key);
                        break;

                    case RedisWorkloadType.HGetAll:
                        client.HGetAll(cmd.hashId);
                        break;

                    case RedisWorkloadType.HMGet:
                        client.HMGet(cmd.hashId, new byte[][] { cmd.key });
                        break;
                }
            }
            return null;
        };

        public static Func<object, object> PIPELINE_ACTION = (object obj) =>
        {
            List<RedisWorkload> commands = (List<RedisWorkload>)obj;
            using (RedisClient client = RedisClientManager.Instance.GetClient(REDIS_DB_INDEX, 0))
            {
                using (IRedisPipeline pipe = client.CreatePipeline())
                {
                    foreach (RedisWorkload cmd in commands)
                    {
                        switch (cmd.type)
                        {
                            case RedisWorkloadType.HSet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(cmd.hashId, cmd.key, cmd.value));
                                break;

                            case RedisWorkloadType.HSetNX:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HSetNX(cmd.hashId, cmd.key, cmd.value));
                                break;

                            case RedisWorkloadType.HMSet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HMSet(cmd.hashId, new byte[][] { cmd.key }, new byte[][] { cmd.value }));
                                break;

                            case RedisWorkloadType.HGet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HGet(cmd.hashId, cmd.key));
                                break;

                            case RedisWorkloadType.HGetAll:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HGetAll(cmd.hashId));
                                break;

                            case RedisWorkloadType.HMGet:
                                pipe.QueueCommand(r => ((RedisNativeClient)r).HMGet(cmd.hashId, new byte[][] { cmd.key }));
                                break;
                        }
                    }
                    pipe.Flush();
                }
            }
            return null;
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
            RedisClientManager manager = RedisClientManager.Instance;
            using (RedisClient client = manager.GetClient(REDIS_DB_INDEX, 0))
            {
                client.FlushDb();
            }
            Console.WriteLine("Flushed the database");

            foreach (Worker worker in workers)
            {
                // non-pipeline mode
                if (!this.pipelineMode)
                {
                    for (int i = 0; i < this.taskCountPerWorker; i++)
                    {
                        worker.EnqueueTxTask(new TxTask(ACTION, this.mockRedisWorkload()));
                    }
                }
                // pipeline mode
                else
                {
                    
                    int batchs = this.taskCountPerWorker / this.pipelineSize;
                    for (int i = 0; i < batchs; i++)
                    {
                        List<RedisWorkload> cmds = new List<RedisWorkload>(this.pipelineSize);
                        for (int j = 0; j < this.pipelineSize; j++)
                        {
                            cmds.Add(this.mockRedisWorkload());
                        }
                        worker.EnqueueTxTask(new TxTask(PIPELINE_ACTION, cmds));
                    }
                }

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

        private RedisWorkload mockRedisWorkload()
        {
            RedisWorkload cmd = new RedisWorkload();
            cmd.hashId = RandomString(4);
            cmd.key = BitConverter.GetBytes(3);
            cmd.value = RandomBytes(50);
            cmd.type = (RedisWorkloadType)RAND.Next(0, 6);
            return cmd;
        }

        private long GetCurrentCommandCount()
        {
            RedisClientManager.Instance.Dispose();

            long commandCount = 0;
            for (int i = 0; i < RedisClientManager.Instance.RedisInstanceCount; i++)
            {
                using (RedisClient redisClient = RedisClientManager.Instance.GetLastestClient(0, 0))
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
