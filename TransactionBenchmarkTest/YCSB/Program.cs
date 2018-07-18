using GraphView.Transaction;
using System;
using System.Diagnostics;
using System.Threading;

namespace TransactionBenchmarkTest.YCSB
{
    internal enum TestType
    {
        Read,
        Update,
        Insert,
    };

    class Program
    {
        private static string[] args;

        static void ExecuteRedisRawTest()
        {
            RedisRawTest.BATCHES = 25000;
            RedisRawTest.REDIS_INSTANCES = 4;

            // ONLY FOR SEPARATE PROGRESS
            RedisRawTest.REDIS_INSTANCES = 1;
            //Console.Write("Input the Redis Id (start from 1): ");
            //string line = Console.ReadLine();
            string line = args[0];
            int redisId = int.Parse(line);
            RedisRawTest.OFFSET = redisId - 1;


            new RedisRawTest().Test();

            Console.Write("Type Enter to close...");
            Console.Read();
        }

        static void RedisBenchmarkTest()
        {
            const int workerCount = 4;
            const int taskCount = 1000000;
            const bool pipelineMode = true;
            const int pipelineSize = 100;

            RedisBenchmarkTest test = new RedisBenchmarkTest(workerCount, taskCount, pipelineMode, pipelineSize);
            test.Setup();
            test.Run();
            test.Stats();
        }

        /// <summary>
        /// For YCSB sync benchmark test
        /// </summary>
        static void YCSBTest()
        {
            const int workerCount = 4;    // 4;
            const int taskCountPerWorker = 2000000;   // 50000;
            const string dataFile = "ycsb_data_lg_r.in";
            const string operationFile = "ycsb_ops_lg_r.in";

            // REDIS VERSION DB
            // VersionDb versionDb = RedisVersionDb.Instance;
            // SINGLETON VERSION DB
            VersionDb versionDb = SingletonVersionDb.Instance();

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCountPerWorker, versionDb);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();
        }

        static void YCSBAsyncTest()
        {
            const int partitionCount = 1;
            const int recordCount = 200000;
            const int executorCount = partitionCount;
            const int txCountPerExecutor = 200000;
            //const bool daemonMode = true;
            const bool daemonMode = false;
            const string dataFile = "ycsb_data_r.in";
            const string operationFile = "ycsb_ops_r.in";
            VersionDb.UDF_QUEUE = false;

            // an executor is responsiable for all flush
            string[] tables =
            {
                YCSBAsyncBenchmarkTest.TABLE_ID,
                VersionDb.TX_TABLE
            };

            // The default mode of versionDb is daemonMode
            SingletonPartitionedVersionDb versionDb = SingletonPartitionedVersionDb.Instance(partitionCount, daemonMode);
            // SingletonVersionDb versionDb = SingletonVersionDb.Instance(executorCount);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                executorCount, txCountPerExecutor, versionDb, tables);
            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();

            Console.WriteLine("Enqueued Requests: {0}", SingletonPartitionedVersionDb.EnqueuedRequests);
            //versionDb.Active = false;
        }

        internal static void PinThreadOnCores()
        {
            Thread.BeginThreadAffinity();
            Process Proc = Process.GetCurrentProcess();
            foreach (ProcessThread pthread in Proc.Threads)
            {
                if (pthread.Id == AppDomain.GetCurrentThreadId())
                {
                    long AffinityMask = (long)Proc.ProcessorAffinity;
                    AffinityMask &= 0x0010;
                    // AffinityMask &= 0x007F;
                    pthread.ProcessorAffinity = (IntPtr)AffinityMask;
                }
            }

            Thread.EndThreadAffinity();
        }


        static void YCSBAsyncTestWithRedisVersionDb(string[] args)
        {
            int redisInstances = 8;
            int partitionCount = RedisVersionDb.PARTITIONS_PER_INSTANCE * redisInstances;
            int executorCount = partitionCount;
            int txCountPerExecutor = 200000;
            const int recordCount = 200000;

            string operationFile = "ycsb_ops_r.in";
            if (args.Length > 1)
            {
                operationFile = args[1];
                partitionCount = Int32.Parse(args[2]);
                executorCount = partitionCount;
                txCountPerExecutor = args.Length > 3 ? Int32.Parse(args[3]) : txCountPerExecutor;
            }

            string[] tables =
            {
                YCSBAsyncBenchmarkTest.TABLE_ID,
                VersionDb.TX_TABLE
            };

            int currentExecutorCount = RedisVersionDb.PARTITIONS_PER_INSTANCE;

            string[] readWriteHosts = new string[]
            {
                //"127.0.0.1:6379",
                "127.0.0.1:6380",
                "127.0.0.1:6381",
                "127.0.0.1:6382",
                "127.0.0.1:6383",
                "127.0.0.1:6384",
                "127.0.0.1:6385",
                "127.0.0.1:6386",
                "127.0.0.1:6387",
            };

            RedisVersionDb versionDb = RedisVersionDb.Instance(currentExecutorCount, readWriteHosts);
            // SingletonVersionDb versionDb = SingletonVersionDb.Instance(1);
            // SingletonPartitionedVersionDb versionDb = SingletonPartitionedVersionDb.Instance(1, true);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                currentExecutorCount, txCountPerExecutor, versionDb, tables);

            test.Setup(operationFile, operationFile);
            for (; currentExecutorCount <= partitionCount; currentExecutorCount += RedisVersionDb.PARTITIONS_PER_INSTANCE)
            {
                if (currentExecutorCount > RedisVersionDb.PARTITIONS_PER_INSTANCE)
                {
                    versionDb.AddPartition(currentExecutorCount);
                }
                test.ResetAndFillWorkerQueue(operationFile, currentExecutorCount);
                test.Run();
                test.Stats();
            }
        }

        // args[0]: dataFile
        // args[1]: opsFile
        // args[2]: partitionCount
        // args[3]: txCountPerExecutor
        static void YCSBAsyncTestWithMemoryVersionDb(string[] args)
        {
            int partitionCount = 4;
            int executorCount = partitionCount;
            int txCountPerExecutor = 200000;

            // 20w
            string dataFile = "ycsb_data_r.in";
            const int recordCount = 1;
            //100w
            //string dataFile = "ycsb_data_m_r.in";
            //const int recordCount = 1000000;
            // 500w
            //string dataFile = "ycsb_data_lg_r.in";
            //const int recordCount = 5000000;
            // 1000w
            //string dataFile = "ycsb_data_hg_r.in";
            //const int recordCount = 10000000;

            string operationFile = "ycsb_ops_r.in";
            if (args.Length > 1)
            {
                dataFile = args[0];
                operationFile = args[1];
                partitionCount = Int32.Parse(args[2]);
                executorCount = partitionCount;
                txCountPerExecutor = args.Length > 3 ? Int32.Parse(args[3]) : txCountPerExecutor;
            }

            // these three settings are useless in SingletonVersionDb environment.
            const bool daemonMode = false;

            string[] tables =
            {
                YCSBAsyncBenchmarkTest.TABLE_ID,
                VersionDb.TX_TABLE
            };

            int currentExecutorCount = 1;

            RedisVersionDb versionDb = RedisVersionDb.Instance();
            // SingletonVersionDb versionDb = SingletonVersionDb.Instance(1);
            // SingletonPartitionedVersionDb versionDb = SingletonPartitionedVersionDb.Instance(1, true);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                currentExecutorCount, txCountPerExecutor, versionDb, tables);

            test.Setup(dataFile, operationFile);
            for (; currentExecutorCount <= partitionCount; currentExecutorCount++)
            {
                if (currentExecutorCount > 1)
                {
                    versionDb.AddPartition(currentExecutorCount);
                }
                test.ResetAndFillWorkerQueue(operationFile, currentExecutorCount);
                test.Run();
                test.Stats();
            }
        }

        //private static bool TEST_ACTIVE = true;
       
        //private static void TestRequestQueue()
        //{
        //    RequestQueue<string> strQueue = new RequestQueue<string>(8);
        //    long beginTicks = DateTime.Now.Ticks;
        //    TEST_ACTIVE = true;

        //    for (int i = 0; i < 8; i++)
        //    {
        //        Task.Factory.StartNew(TestEnqueue, strQueue);
        //    }
        //    Task.Factory.StartNew(TestDequeue, strQueue);

        //    while (DateTime.Now.Ticks - beginTicks < 1 * 10000000) ;
        //    TEST_ACTIVE = false;
        //}

        //private static Action<object> TestEnqueue = (object obj) =>
        //{
        //    RequestQueue<string> strQueue = obj as RequestQueue<string>;
        //    Random rand = new Random();
        //    while (TEST_ACTIVE)
        //    {
        //        int pk = rand.Next(0, 8);
        //        strQueue.Enqueue("123", pk);
        //    }
        //};

        //private static Action<object> TestDequeue = (object obj) =>
        //{
        //    RequestQueue<string> strQueue = obj as RequestQueue<string>;
        //    Random rand = new Random();
        //    string value = null;
        //    while (TEST_ACTIVE)
        //    {
        //        int pk = rand.Next(0, 8);
        //        if (strQueue.TryDequeue(out value))
        //        {
        //            Debug.Assert(value != null);
        //        }
        //    }
        //};

        public static void Main(string[] args)
        {
            Program.args = args;
            // For the YCSB sync test
            // YCSBTest();
            // YCSBSyncTestWithCassandra();
            // test_cassandra();

            // For the redis benchmark Test
            // RedisBenchmarkTest();

            // For the YCSB async test
            // YCSBAsyncTest();
            YCSBAsyncTestWithRedisVersionDb(args);
            // YCSBAsyncTestWithMemoryVersionDb(args);
            // YCSBAsyncTestWithPartitionedVersionDb(args);
            // YCSBAsyncTestWithCassandra();

            // ExecuteRedisRawTest();
        }
    }
}