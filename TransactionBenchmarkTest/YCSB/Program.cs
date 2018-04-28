using System;
using System.Diagnostics;
using System.Threading;
using ServiceStack.Redis;

namespace TransactionBenchmarkTest.YCSB
{
    class Program
    {
        static void RedisBenchmarkTest()
        {
            const int workerCount = 1;
            const int taskCount = 500000;
            const bool pipelineMode = false;
            const int pipelineSize = 1000;

            RedisBenchmarkTest test = new RedisBenchmarkTest(workerCount, taskCount, pipelineMode, pipelineSize);
            test.Setup();
            test.Run();
            test.Stats();
        }

        static void YCSBTest()
        {
            const int workerCount = 4;      // 4;
            const int taskCount = 25000;   // 50000;
            const string dataFile = "ycsb_data_u.in";
            const string operationFile = "ycsb_ops_u_shuffle.in";

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCount);
            test.Setup(dataFile, operationFile);

            //Console.WriteLine("PLEASE INPUT RETURN TO CONTINUE...");
            //Console.Read();

            test.Run();
            test.Stats();
        }

        static void TxOnlyTest()
        {
            const int workerCount = 8;      // 4;
            const int taskCount = 25000;   // 50000;

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCount);
            test.FlushRedis();

            //Console.WriteLine("PLEASE INPUT RETURN TO CONTINUE...");
            //Console.Read();

            test.RunTxOnly();
            
            test.Stats();
        }

        static void YCSBReadOnlyTest()
        {
            const int workerCount = 4;      // 4;
            const int taskCount = 50000;   // 50000;
            const string dataFile = "ycsb_data_r.i";
            const string operationFile = "ycsb_ops_r.in";

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCount);
            test.SetupReadOnly(dataFile, operationFile);

            //Console.WriteLine("PLEASE INPUT RETURN TO CONTINUE...");
            //Console.Read();

            test.Run();
            test.Stats();
        }

        static void YCSBAsyncTest()
        {
            const int executorCount = 1;
            const int txCountPerExecutor = 50000;
            const string dataFile = "ycsb_data.in";
            const string operationFile = "ycsb_ops.in";

            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(executorCount, txCountPerExecutor);
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

            // TxOnlyTest();

            //YCSBReadOnlyTest();

            Console.WriteLine("Done");
            Console.Read();
            YCSBAsyncTest();
        }
        
    }
}
