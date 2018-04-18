using GraphView.Transaction;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TransactionBenchmarkTest.YCSB
{
    class Program
    {
        static void RunWorkerDemo()
        {
            // start a worker
            Worker worker = new Worker(1);
            worker.Active = true;
            Thread thread = new Thread(new ThreadStart(worker.Monitor));
            thread.Start();

            Func<object, object> action = (object obj) =>
            {
                Console.WriteLine("Thread {0} is running", Thread.CurrentThread.ManagedThreadId);
                Tuple<int, int> tuple = new Tuple<int, int>((int)obj, 123);
                return (object)tuple;
            };

            List<Task<object>> taskList = new List<Task<object>>();
            for (int i = 0; i < 5; i++)
            {
                Task<object> t = new Task<object>(action, i);
                taskList.Add(t);
                worker.EnqueueTxTask(t);
            }

            Thread.Sleep(1000);
            for (int i = 0; i < 5; i++)
            {
                if (taskList[i].IsCompleted)
                {
                    Console.WriteLine(taskList[i].Result);
                }
            }
            // close the worker
            worker.Active = false;
        }

        static void YCSBTest()
        {
            // RedisVersionDb.Instance.PipelineMode = true;
            const int workerCount = 5;
            const string dataFile = "ycsb_data.in";
            const string operationPrefix = "ycsb_ops";

            ThroughputBenchmarkTest test = new ThroughputBenchmarkTest(workerCount);
            // step1: setup test data
            test.SetupTest(dataFile);

            // step2: fill workers' queue
            test.FillWorkersQueue(operationPrefix);

            // RedisVersionDb.Instance.PipelineMode = true;
            // step3: run test
            test.Run();

            //Console.WriteLine("The main thread is waiting 30s.");
            //Thread.Sleep(10000);

            // step4: output the test information
            test.Conclude();

            // close the daemon threads
            test.Dispose();

            // Console.WriteLine("TOTAL_FLUSH: {0}, TIME_FLUSH: {1}, COMMANDS_FLUSH: {2}", 
            // RedisConnectionPool.TOTAL_FLUSH, RedisConnectionPool.TIME_FLUSH, RedisConnectionPool.COMMANDS_FLUSHED);
            // Console.WriteLine("FINISHED TXS: {0}, COMMITED TXS: {1}", ThroughputBenchmarkTest.FINISHED_TXS, ThroughputBenchmarkTest.COMMITED_TXS);
        }

        public static void Main(string[] args)
        {
            YCSBTest();
        }
    }
}
