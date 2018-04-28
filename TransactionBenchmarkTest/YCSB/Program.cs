using GraphView.Transaction;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace TransactionBenchmarkTest.YCSB
{
    class Program
    {
        static void RedisBenchmarkTest()
        {
            const int workerCount = 5;
            const int taskCount = 10000;
            const bool pipelineMode = false;
            const int pipelineSize = 1000;

            RedisBenchmarkTest test = new RedisBenchmarkTest(workerCount, taskCount, pipelineMode, pipelineSize);
            test.Setup();
            test.Run();
            test.Stats();
        }

        static void YCSBTest()
        {
            const int workerCount = 4;
            const int taskCount = 50000;
            const string dataFile = "ycsb_data.in";
            const string operationFile = "ycsb_ops.in";

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCount);
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
            //byte[] bytes = BitConverter.GetBytes(5L);
            //object value = BitConverter.ToInt64(bytes, 0);
            //long longv = Convert.ToInt64(value);

            // PinThreadOnCores();
            // YCSBTest();
            // RedisBenchmarkTest();
            YCSBAsyncTest();
        }
    }
}
