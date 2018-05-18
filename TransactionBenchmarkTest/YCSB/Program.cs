using GraphView.Transaction;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace TransactionBenchmarkTest.YCSB
{
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
			VersionDb versionDb = SingletonVersionDb.Instance;

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCountPerWorker, versionDb);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();
        }

        static void YCSBAsyncTest()
        {
            const int partitionCount = 1;
            const int recordCount = 0;
            const int executorCount = 4;
            const int txCountPerExecutor = 1500000;
            const bool daemonMode = true;
            const string dataFile = "ycsb_data_lg_r.in";
            const string operationFile = "ycsb_ops_lg_r.in";

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>();

            // The default mode of versionDb is daemonMode
            SingletonPartitionedVersionDb versionDb = SingletonPartitionedVersionDb.Instance(partitionCount, daemonMode);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount, 
                executorCount, txCountPerExecutor, versionDb, instances);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();

            versionDb.Active = false;
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

        public static void Main(string[] args)
        {
            Program.args = args;
            // For the YCSB sync test
            // YCSBTest();

            // For the redis benchmark Test
            // RedisBenchmarkTest();

            // For the YCSB async test
            YCSBAsyncTest();

            // ExecuteRedisRawTest();
        }
    }
}
