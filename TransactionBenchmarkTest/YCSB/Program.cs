using System;
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
            const int workerCount = 6;
            const int taskCount = 1000;
            const string dataFile = "ycsb_data.in";
            const string operationFile = "ycsb_ops.in";

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCount);
            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();
        }

        public static void Main(string[] args)
        {
            YCSBTest();
            // RedisBenchmarkTest();
        }
    }
}
