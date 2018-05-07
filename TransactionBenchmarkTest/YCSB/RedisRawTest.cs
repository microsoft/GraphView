namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;
    using System;
    using System.Collections.Generic;
    using System.Threading;

    class RedisRawTest
    {
        private static IRedisClient[] clients;

        internal static int BATCHES = 0;

        internal static int REDIS_INSTANCES = 1;

        internal static int THREAD_PER_REDIS = 3;

        protected static class StaticRandom
        {
            static int seed = Environment.TickCount;

            static readonly ThreadLocal<Random> random =
                new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref seed)));

            public static long RandIdentity()
            {
                byte[] buf = new byte[8];
                random.Value.NextBytes(buf);
                long longRand = BitConverter.ToInt64(buf, 0);

                return Math.Abs(longRand);
            }
        }

        internal static string[] READ_WRITE_HOST =
        {
            "127.0.0.1:6379",
            //"127.0.0.1:6381",
            //"127.0.0.1:6382",
            //"127.0.0.1:6383",
            //"127.0.0.1:6384",
            //"127.0.0.1:6385",
            //"127.0.0.1:6386",
            //"127.0.0.1:6387",
            //"127.0.0.1:6388",
            //"127.0.0.1:6389",
            //"127.0.0.1:6390",
            //"127.0.0.1:6390",
            //"127.0.0.1:6391",
            //"127.0.0.1:6392",
            //"127.0.0.1:6393",
            //"127.0.0.1:6394",
            //"127.0.0.1:6395",
        };

        private static IRedisClientsManager[] MANAGER;

        public static void Init()
        {
            MANAGER = new IRedisClientsManager[REDIS_INSTANCES];
            clients = new IRedisClient[REDIS_INSTANCES * THREAD_PER_REDIS];

            RedisClientManagerConfig config = new RedisClientManagerConfig();
            config.DefaultDb = 0;
            config.MaxReadPoolSize = 5;
            config.MaxWritePoolSize = 5;

            for (int i = 0; i < REDIS_INSTANCES;i++)
            {
                MANAGER[i] = new PooledRedisClientManager(
                    new string[] { READ_WRITE_HOST[i] },
                    new string[] { READ_WRITE_HOST[i] },
                    config);
                for (int j = 0; j < THREAD_PER_REDIS; j++)
                {
                    clients[i * THREAD_PER_REDIS + j] = MANAGER[i].GetClient();
                }
            }
        }

        public static void Run(object obj)
        {
            Tuple<int, int> tuple = (Tuple<int, int>)obj;
            int partition = tuple.Item1, clientIndex = tuple.Item2;
            IRedisClient redisClient = clients[clientIndex];
            
            for (int i = 0; i < BATCHES; i++)
            {
                using (IRedisPipeline pipe = redisClient.CreatePipeline())
                {
                    string hashId = StaticRandom.RandIdentity().ToString();
                    byte[] key = BitConverter.GetBytes(StaticRandom.RandIdentity());
                    for (int j = 0; j < 100; j++)
                    {
                        pipe.QueueCommand(r => ((RedisNativeClient)r).HGet(hashId, key));
                    }
                    pipe.Flush();
                }
            }
        }

        public void Test()
        {
            Init();
            Console.WriteLine("Initied");

            List<Thread> threadList = new List<Thread>();
            int totalThreads = THREAD_PER_REDIS * REDIS_INSTANCES;

            Console.WriteLine("Running...");
            long beginTicks = DateTime.Now.Ticks;
            for (int i = 0; i < totalThreads; i++)
            {
                Thread thread = new Thread(new ParameterizedThreadStart(Run));
                threadList.Add(thread);
                thread.Start(Tuple.Create(i/THREAD_PER_REDIS,i));
            }

            foreach (Thread thread in threadList)
            {
                thread.Join();
            }

            long endTicks = DateTime.Now.Ticks;

            int totalCommands = totalThreads * BATCHES * 100;
            double seconds = (endTicks - beginTicks) * 1.0 / 10000000;
            int throughput = (int)(totalCommands / seconds);
            Console.WriteLine("Finished {0} commands in {1} seconds", totalCommands, seconds);
            Console.WriteLine("Throughput: {0}", throughput);
        }
    }
}
