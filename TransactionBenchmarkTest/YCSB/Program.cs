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
        public static void Main(string[] args)
        {
            // start a worker
            Worker worker = new Worker();
            worker.Active = true;
            Thread thread = new Thread(new ThreadStart(worker.Monitor));
            thread.Start();

            Func<object, object> action = (object obj) =>
            {
                Console.WriteLine("Thread {0} is running", Thread.CurrentThread.ManagedThreadId);
                Tuple<int, int> tuple = new Tuple<int, int>((int) obj, 123);
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
    }
}
