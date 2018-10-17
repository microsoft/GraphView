using GraphView.Transaction;
using NDesk.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TransactionBenchmarkTest.YCSB;

namespace TransactionBenchmarkTest
{
    class RawDictionaryBenchmark
    {
        private readonly Dictionary<long, Dictionary<long, VersionEntry>> dictionary;

        private int innerCapacity;
        private int threadCount;
        private int workLoadPerThread;
        private int recordCount;
        private Distribution distribution;
        private double theta;
        private int capacity;

        public RawDictionaryBenchmark(int threadCount, int workLoadPerThread, Distribution distribution,
            double theta, int recordCount = 1000000, int capacity = 10000, int innerCapacity = 100)
        {
            this.threadCount = threadCount;
            this.workLoadPerThread = workLoadPerThread;
            this.distribution = distribution;
            this.theta = theta;
            this.recordCount = recordCount;
            this.dictionary = new Dictionary<long, Dictionary<long, VersionEntry>>();
            this.capacity = capacity;
            this.innerCapacity = innerCapacity;
        }

        public void LoadData(int listCount)
        {
            for (int i = 0; i < recordCount; i++)
            {
                Dictionary<long, VersionEntry> entryList = GenerateVersoinList(listCount);
                dictionary.Add(i, entryList);
            }
        }

        private Dictionary<long, VersionEntry> GenerateVersoinList(int listCount)
        {
            Dictionary<long, VersionEntry> entryList = new Dictionary<long, VersionEntry>();
            for (int i = 0; i < listCount; i++)
            {
                VersionEntry entry = new VersionEntry(i, new String('a', 100), -1);
                entryList.Add(i, entry);
            }
            return entryList;
        }

        public void printArguments()
        {
            Console.WriteLine("worker: {0}", this.threadCount);
            Console.WriteLine("work load per worker: {0}", this.workLoadPerThread);
            Console.WriteLine("record count: {0}", this.recordCount);
            Console.WriteLine("Distribution: {0}", distribution.ToString());
            Console.WriteLine("theta value: {0}", this.theta);
            Console.WriteLine("dictionary capacity : {0}", this.capacity);
            Console.WriteLine("version list dictionary capacity : {0}", this.innerCapacity);      
        }

        public void Run()
        {
            printArguments();
            LoadData(5);
            List<Worker> workerList = new List<Worker>();
            List<Thread> threadList = new List<Thread>();

            for (int i = 0; i < threadCount; i++)
            {
                Worker worker = new Worker(dictionary, workLoadPerThread, recordCount, distribution, theta);
                workerList.Add(worker);
                Thread thread = new Thread(new ThreadStart(worker.Load));
                threadList.Add(thread);
                thread.Start();
            }

            foreach (Thread t in threadList)
            {
                t.Join();
            }

            threadList.Clear();

            foreach (Worker worker in workerList)
            {
                Thread thread = new Thread(new ThreadStart(worker.Run));
                threadList.Add(thread);
            }

            ForceGC();

            foreach (Thread t in threadList)
            {
                t.Start();
            }

            PrintStats(workerList);

        }

        private void PrintStats(List<Worker> workList)
        {
            long lastTime = DateTime.Now.Ticks;
            long now;
            int lastSum = 0;
            int sum = 0;
            int thread = 0;
            foreach (Worker worker in workList)
            {
                lastSum += worker.Count;
            }

            bool isFinished = false;

            while (!isFinished)
            {
                Thread.Sleep(100);
                sum = 0;
                isFinished = true;
                thread = 0;
                foreach (Worker worker in workList)
                {
                    sum += worker.Count;
                    if (!worker.IsFinished)
                    {
                        thread++;
                        isFinished = false;
                    }
                }
                now = DateTime.Now.Ticks;
                double runSeconds = (now - lastTime) / 10000000.0;
                Console.WriteLine("Time Inteval : {0}, Total Finish Count: {1}, Throughput: {2} txn/s, thread : {3}",
                    runSeconds, sum, (sum - lastSum) / runSeconds, thread);
                lastSum = sum;
                lastTime = now;
            }
        }
        static double TotalMemoryInMB()
        {
            return GC.GetTotalMemory(false) / 1024.0 / 1024.0;
        }

        static void ForceGC()
        {
            Console.WriteLine($"Before GC: {TotalMemoryInMB():F3}MB is used");
            GC.Collect();
            Console.WriteLine($"After GC: {TotalMemoryInMB():F3}MB is used");
        }


        class Worker
        {
            private readonly Dictionary<long, Dictionary<long, VersionEntry>> dictionary;
            private readonly long[] keys;
            private volatile bool isFinished = false;
            private volatile int count = 0;
            private int workLoad;
            private YCSBDataGenerator dataGenerator;
            public Worker(Dictionary<long, Dictionary<long, VersionEntry>> dictionary, int workLoad,
                int recordCount, Distribution dist, double theta)
            {
                this.dictionary = dictionary;
                this.workLoad = workLoad;
                keys = new long[workLoad];
                dataGenerator = new YCSBDataGenerator(recordCount, 1, dist, theta);

            }

            public void Load()
            {
                for (int i = 0; i < workLoad; i++)
                {
                    keys[i] = dataGenerator.NextIntKey();
                }
            }

            public void Run()
            {
                Dictionary<long, VersionEntry> list = null;
                VersionEntry entry = null;
                for (int i = 0; i < workLoad; i++)
                {
                    list = dictionary[keys[i]];
                    if (list != null)
                    {
                        entry = list[0];
                    }
                    count++;
                }
                isFinished = true;
            }

            public bool IsFinished
            {
                get { return isFinished; }
            }

            public int Count
            {
                get { return count; }
            }
        }


        public static void Main(String[] args)
        {
            int recordCount = 1000000;
            int workerCount = 1;
            int workloadCount = 1000000;
            Distribution distribution = Distribution.Zipf;
            double theta = 0.8;
            int capacity = 1000000;
            int innerCapacity = 100;

            OptionSet optionSet = new OptionSet()
            {
                {
                    "r|record=", "the number of records", v => recordCount = int.Parse(v)
                },
                {
                    "w|workload=", "the number of operations per worker", v => workloadCount = int.Parse(v)
                },
                {
                    "W|worker=", "the number of workers", v => workerCount = int.Parse(v)
                },
                 {
                    "d|dist=", "the distribution of generated keys", v =>
                    {
                        switch(v)
                        {
                            case "uniform":
                                distribution = Distribution.Uniform;
                                break;
                            case "zipf":
                                distribution = Distribution.Zipf;
                                break;
                            default:
                                throw new ArgumentException("wrong key distribution");
                        }
                    }
                },
                {
                    "t|theta=", "the theta option of zipf distribution", v => theta = double.Parse(v)
                },
                {
                    "c|capacity=", "the initial capacity of the dictionary ", v => capacity = int.Parse(v)
                },
                {
                    "ic|innercapacity=", "the initial capacity of the dictionary ", v => innerCapacity = int.Parse(v)
                }
            };
            optionSet.Parse(args);
            RawDictionaryBenchmark benchmark = new RawDictionaryBenchmark(workerCount, workloadCount,
                distribution, theta, recordCount, capacity, innerCapacity);
            benchmark.Run();
    }
    }
}
