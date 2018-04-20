namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    class RedisCommand
    {
        internal string hashId;
        internal byte[] key;
        internal byte[] value;
        internal int type;
    }

    class RedisBenchmarkTest
    {
        public static readonly long REDIS_DB_INDEX = 3L;

        public static Func<object, object> ACTION = (object obj) =>
        {
            RedisCommand cmd = (RedisCommand)obj;
            using (RedisClient client = RedisClientManager.Instance.GetClient(REDIS_DB_INDEX, 0))
            {
                if (cmd.type == 0)
                {
                    client.HSet(cmd.hashId, cmd.key, cmd.value);
                }
                else
                {
                    client.HGet(cmd.hashId, cmd.key);
                }
            }
            return null;
        };

        public static Func<object, object> PIPELINE_ACTION = (object obj) =>
        {
            List<RedisCommand> commands = (List<RedisCommand>)obj;
            using (RedisClient client = RedisClientManager.Instance.GetClient(REDIS_DB_INDEX, 0))
            {
                using (IRedisPipeline pipe = client.CreatePipeline())
                {
                    foreach (RedisCommand cmd in commands)
                    {
                        if (cmd.type == 0)
                        {
                            pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(cmd.hashId, cmd.key, cmd.value));
                        }
                        else
                        {
                            pipe.QueueCommand(r => ((RedisNativeClient)r).HGet(cmd.hashId, cmd.key));
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

        internal int Throughput
        {
            get
            {
                double runSeconds = ((this.testEndTicks - this.testBeginTicks) * 1.0) / 10000000;
                int taskCount = this.workerCount * this.taskCountPerWorker;
                Console.WriteLine("Finshed {0} requests in {1} seconds", taskCount, runSeconds);
                return (int) (taskCount / runSeconds);
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
                this.workers.Add(new Worker(i+1, Math.Max(taskCount, Worker.DEFAULT_QUEUE_SIZE)));
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
                        worker.EnqueueTxTask(new TxTask(ACTION, this.mockRedisCommands()));
                    }
                }
                // pipeline mode
                else
                {
                    int batchs = this.taskCountPerWorker / this.pipelineSize;
                    for (int i = 0; i < batchs; i++)
                    {
                        List<RedisCommand> cmds = new List<RedisCommand>(this.pipelineSize);
                        for (int j = 0; j < this.pipelineSize; j++)
                        {
                            cmds.Add(this.mockRedisCommands());
                        }
                        worker.EnqueueTxTask(new TxTask(PIPELINE_ACTION, cmds));
                    }
                }
                
            }
            Console.WriteLine("Filled the workers' queue");
        }

        internal void Run()
        {
            Console.WriteLine("Try to run {0} tasks in {1} workers", (this.workerCount * this.taskCountPerWorker),this.workerCount);
            Console.WriteLine("Running......");

            this.testBeginTicks = DateTime.Now.Ticks;
            List<Thread> threadList = new List<Thread>();

            foreach(Worker worker in this.workers)
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

        private RedisCommand mockRedisCommands()
        {
            RedisCommand cmd = new RedisCommand();
            cmd.hashId = RandomString(4);
            cmd.key = BitConverter.GetBytes(3);
            cmd.value = RandomBytes(50);
            cmd.type = RAND.Next(0, 1);
            return cmd;
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
