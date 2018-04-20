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
            const int workerCount = 10;
            const int taskCount = 250000;
            const bool pipelineMode = true;
            const int pipelineSize = 100;

            RedisBenchmarkTest test = new RedisBenchmarkTest(workerCount, taskCount, pipelineMode, pipelineSize);
            test.Setup();
            test.Run();
            Console.WriteLine("Redis Throughput: {0} requests/second", test.Throughput);
        }

        static void YCSBTest()
        {
            const int workerCount = 10;
            const int taskCount = 1000;
            const string dataFile = "ycsb_data.in";
            const string operationFile = "ycsb_ops.in";

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCount);
            test.Setup(dataFile, operationFile);
            test.Run();
            Console.WriteLine("Transaction Throughput: {0} tx/second", test.Throughput);
        }

        public static void Main(string[] args)
        {
            YCSBTest();
            //RedisBenchmarkTest();
            Console.Read();
        }
    }
}
