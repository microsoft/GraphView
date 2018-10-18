using GraphView.Transaction;
using NDesk.Options;
using NonBlocking;
using System;
using System.Collections.Generic;
using System.Threading;
using TransactionBenchmarkTest.YCSB;

namespace TransactionBenchmarkTest
{

    class DictionaryFactory
    {
        public static IDictionaryComponent newInstance(string type)
        {
            switch (type)
            {
                case "simple":
                    return new SimpleDicionary();
                case "concurrent":
                    return new ConcurrentDictionary();
                case "mix":
                    return new OuterConcurrentDicionary();
                default:
                    throw new ArgumentException($"wrong type {type}");
            }

        }
    }

    interface IDictionaryComponent
    {
        void Init(int capacity, int innerCapacity, int recordCount);
        void RandomAcess(long key);
        void LoadData(int recordCount);
    }

    class SimpleDicionary : IDictionaryComponent
    {
        private Dictionary<long, Dictionary<long, VersionEntry>> dictionary;
        private int capacity;
        private int innerCapacity;

        public void Init(int capacity, int innerCapacity, int recordCount)
        {
            this.capacity = capacity;
            this.innerCapacity = innerCapacity;
            dictionary = new Dictionary<long, Dictionary<long, VersionEntry>>(capacity);
            LoadData(recordCount);
        }

        public void LoadData(int recordCount)
        {
            for (int i = 0; i < recordCount; i++)
            {
                Dictionary<long, VersionEntry> entryList = GenerateVersoinList(5);
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

        public void RandomAcess(long key)
        {
            Dictionary<long, VersionEntry> list = dictionary[key];
            if (list != null)
            {
                VersionEntry entry = list[0];
            }
        }
    }

    class ConcurrentDictionary : IDictionaryComponent
    {
        private ConcurrentDictionary<long, ConcurrentDictionary<long, VersionEntry>> dictionary;
        private int capacity;
        private int innerCapacity;

        public void Init(int capacity, int innerCapacity, int recordCount)
        {
            this.capacity = capacity;
            this.innerCapacity = innerCapacity;
            dictionary = new ConcurrentDictionary<long, ConcurrentDictionary<long, VersionEntry>>();
            LoadData(recordCount);
        }

        public void LoadData(int recordCount)
        {
            for (int i = 0; i < recordCount; i++)
            {
                ConcurrentDictionary<long, VersionEntry> entryList = GenerateVersoinList(5);
                dictionary.Add(i, entryList);
            }
        }

        private ConcurrentDictionary<long, VersionEntry> GenerateVersoinList(int listCount)
        {
            ConcurrentDictionary<long, VersionEntry> entryList = new ConcurrentDictionary<long, VersionEntry>();
            for (int i = 0; i < listCount; i++)
            {
                VersionEntry entry = new VersionEntry(i, new String('a', 100), -1);
                entryList.Add(i, entry);
            }
            return entryList;
        }

        public void RandomAcess(long key)
        {
            ConcurrentDictionary<long, VersionEntry> list = dictionary[key];
            if (list != null)
            {
                VersionEntry entry = list[0];
            }
        }
    }

    class OuterConcurrentDicionary : IDictionaryComponent
    {
        private ConcurrentDictionary<long, Dictionary<long, VersionEntry>> dictionary;
        private int capacity;
        private int innerCapacity;

        public void Init(int capacity, int innerCapacity, int recordCount)
        {
            this.capacity = capacity;
            this.innerCapacity = innerCapacity;
            dictionary = new ConcurrentDictionary<long, Dictionary<long, VersionEntry>>();
            LoadData(recordCount);
        }

        public void LoadData(int recordCount)
        {
            for (int i = 0; i < recordCount; i++)
            {
                Dictionary<long, VersionEntry> entryList = GenerateVersoinList(5);
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

        public void RandomAcess(long key)
        {
            Dictionary<long, VersionEntry> list = dictionary[key];
            if (list != null)
            {
                VersionEntry entry = list[0];
            }
        }
    }

    class RawDictionaryBenchmark
    {
        private IDictionaryComponent dictionary;

        private int innerCapacity;
        private int threadCount;
        private int workLoadPerThread;
        private int recordCount;
        private Distribution distribution;
        private double theta;
        private int capacity;
        private string type;

        public RawDictionaryBenchmark(string type, int threadCount, int workLoadPerThread, Distribution distribution,
            double theta, int recordCount = 1000000, int capacity = 10000, int innerCapacity = 100)
        {
            this.threadCount = threadCount;
            this.workLoadPerThread = workLoadPerThread;
            this.distribution = distribution;
            this.theta = theta;
            this.recordCount = recordCount;
            this.dictionary = DictionaryFactory.newInstance(type);
            this.dictionary.Init(capacity, innerCapacity, recordCount);
            this.capacity = capacity;
            this.innerCapacity = innerCapacity;
            this.type = type;
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
            Console.WriteLine("dictionary type : {0}", this.type);
        }

        public void Run()
        {
            printArguments();
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
            private IDictionaryComponent dictionary;
            private readonly long[] keys;
            private volatile bool isFinished = false;
            private volatile int count = 0;
            private int workLoad;
            private YCSBDataGenerator dataGenerator;
            public Worker(IDictionaryComponent dictionary, int workLoad,
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
                for (int i = 0; i < workLoad; i++)
                {
                    this.dictionary.RandomAcess(keys[i]);
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
            string type = "simple";

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
                },
                {
                    "y|type=", "the type of the dictionary ", v => type = v
                }
            };
            optionSet.Parse(args);
            RawDictionaryBenchmark benchmark = new RawDictionaryBenchmark(type, workerCount, workloadCount,
                distribution, theta, recordCount, capacity, innerCapacity);
            benchmark.Run();
    }
    }
}
