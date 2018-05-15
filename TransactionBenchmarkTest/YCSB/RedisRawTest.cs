namespace TransactionBenchmarkTest.YCSB
{
    using ServiceStack.Redis;
    using ServiceStack.Redis.Pipeline;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    class RedisRawTest
    {
        private static IRedisClient[] clients;

        internal static int BATCHES = 0;

        internal static int REDIS_INSTANCES = 1;

        internal static int THREAD_PER_REDIS = 3;

        internal static int OFFSET = 0;

        internal static ManualResetEventSlim SLIM = new ManualResetEventSlim();

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
            "127.0.0.1:6380",
            "127.0.0.1:6381",
            "127.0.0.1:6382",
            "127.0.0.1:6383",
            "127.0.0.1:6384",
            "127.0.0.1:6385",
            "127.0.0.1:6386",
            "127.0.0.1:6387",
            "127.0.0.1:6388",
            "127.0.0.1:6389",
            "127.0.0.1:6390",
            "127.0.0.1:6390",
            "127.0.0.1:6391",
            "127.0.0.1:6392",
            "127.0.0.1:6393",
            "127.0.0.1:6394",
            "127.0.0.1:6395",
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
                    new string[] { READ_WRITE_HOST[i + OFFSET] },
                    new string[] { READ_WRITE_HOST[i + OFFSET] },
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
            // PinThreadOnCores(clientIndex);

            // debug
            Console.WriteLine("{0}:{1}", redisClient.Host, redisClient.Port);

            SLIM.Wait();
            long beginTicks = DateTime.Now.Ticks;
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
            long endTicks = DateTime.Now.Ticks;

            int throughput = (int)(BATCHES * 100 * 1.0 / ((endTicks - beginTicks) * 1.0 / 10000000));
            // Console.WriteLine("Single Thread Throughput: {0}", throughput);
        }

        internal static void PinThreadOnCores(long coreIndex)
        {
            long allowMask = (1L << (int)coreIndex);
            // Console.WriteLine(Convert.ToString(allowMask, 2).PadLeft(32, '0'));
            Console.WriteLine("Running on the {0}-th core", coreIndex);
            Thread.BeginThreadAffinity();
            Process Proc = Process.GetCurrentProcess();
            foreach (ProcessThread pthread in Proc.Threads)
            {
                if (pthread.Id == AppDomain.GetCurrentThreadId())
                {
                    long AffinityMask = (long)Proc.ProcessorAffinity;
                    AffinityMask &= allowMask;
                    // AffinityMask &= 0x007F;
                    pthread.ProcessorAffinity = (IntPtr)AffinityMask;
                }
            }

            Thread.EndThreadAffinity();
        }

        public void Test()
        {
            Init();
            Console.WriteLine("Initied");

            int totalThreads = THREAD_PER_REDIS * REDIS_INSTANCES;
            Task[] workers = new Task[totalThreads];
            // ManualResetEventSlim e = new ManualResetEventSlim();

            for (int i = 0; i < workers.Length; i++)
            {
                int threadIndex = i;
                workers[i] = Task.Factory.StartNew(
                    () => Run(Tuple.Create(threadIndex / THREAD_PER_REDIS, threadIndex)), TaskCreationOptions.LongRunning);
            }

            Console.WriteLine("Running...");
            long beginTicks = DateTime.Now.Ticks;
            SLIM.Set();
            Task.WaitAll(workers);
            long endTicks = DateTime.Now.Ticks;

            int totalCommands = totalThreads * BATCHES * 100;
            double seconds = (endTicks - beginTicks) * 1.0 / 10000000;
            int throughput = (int)(totalCommands / seconds);
            Console.WriteLine("Finished {0} commands in {1} seconds", totalCommands, seconds);
            Console.WriteLine("Throughput: {0}", throughput);
        }
    }
}
