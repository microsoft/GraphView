using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest.YCSB
{
    class Program
    {
        static void RedisBenchmarkTest()
        {
            const int workerCount = 8;
            const int taskCount = 500000;
            const bool pipelineMode = false;
            const int pipelineSize = 100;

            RedisBenchmarkTest test = new RedisBenchmarkTest(workerCount, taskCount, pipelineMode, pipelineSize);
            test.Setup();
            test.Run();
            Console.WriteLine("Redis Throughput: {0} requests/second", test.Throughput);
        }

        static void YCSBTest()
        {
            const int workerCount = 5;
            const int taskCount = 2000;
            const string dataFile = "ycsb_data.in";
            const string operationFile = "ycsb_ops.in";

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCount);
            test.Setup(dataFile, operationFile);
            test.Run();
            Console.WriteLine("Transaction Throughput: {0} tx/second", test.Throughput);
        }

        public static void Main(string[] args)
        {
            // YCSBTest();
            RedisBenchmarkTest();
        }
    }
}
