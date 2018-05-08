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
            const int taskCount = 2000000;
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
            const int workerCount = 4;      // 4;
            const int taskCountPerWorker = 25000;   // 50000;
            const string dataFile = "ycsb_data_r.in";
            const string operationFile = "ycsb_ops_r.in";
            VersionDb versionDb = RedisVersionDb.Instance;

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCountPerWorker, versionDb);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();
        }

        static void YCSBAsyncTest()
        {
            const int executorCount = 4;
            const int txCountPerExecutor = 50000;
            const string dataFile = "ycsb_data.in";
            const string operationFile = "ycsb_ops.in";

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>
            {
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                },
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                }
            };

            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(executorCount, txCountPerExecutor, instances);
            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();
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
            // YCSBAsyncTest();

            ExecuteRedisRawTest();
        }
    }
}
