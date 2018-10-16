using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using GraphView.Transaction;

namespace TransactionBenchmarkTest.YCSB
{
    static public class SingletonYCSBTxExecHelper
    {
        static internal TransactionExecution Insert(
            this TransactionExecution self, object key, object val)
        {
            self.InitAndInsert(YcsbHelper.YCSB_TABLE, key, val);
            return self;
        }
        static internal TransactionExecution Read(
            this TransactionExecution self, object key)
        {
            self.SyncRead(YcsbHelper.YCSB_TABLE, key);
            return self;
        }
        static internal TransactionExecution ReadThenUpdate(
            this TransactionExecution self, object key, object value)
        {
            self.SyncRead(YcsbHelper.YCSB_TABLE, key);
            self.Update(YcsbHelper.YCSB_TABLE, key, value);
            return self;
        }
        static internal bool IsAborted(this TransactionExecution self)
        {
            return self.TxStatus == TxStatus.Aborted;
        }
        static internal TransactionExecution MakeTxExec(
            this VersionDb versionDb, int workerId = 0)
        {
            return new TransactionExecution(
                null, versionDb, null, new TxRange(workerId),
                workerId % versionDb.PartitionCount);
        }
        static internal
        TransactionExecution[] MakeTxExecs(VersionDb versionDb)
        {
            var txExecs = new TransactionExecution[versionDb.PartitionCount];
            for (int i = 0; i < versionDb.PartitionCount; ++i)
            {
                txExecs[i] = versionDb.MakeTxExec(i);
            }
            return txExecs;
        }
    }

    static public class YcsbHelper
    {
        public const string YCSB_TABLE = "YCSB";

        static internal SingletonVersionDb MakeVersionDb(int concurrency)
        {
            var versionDb = SingletonVersionDb.Instance(concurrency);
            versionDb.CreateVersionTable(YCSB_TABLE);
            return versionDb;
        }

        static public object NewPayload()
        {
            return new String('a', 100);
        }
        static public object UpdatePayload()
        {
            return YcsbHelper.UPDATE_PAYLOAD;
        }
        static string UPDATE_PAYLOAD = new string('a', 100);
    }
    public struct YcsbQuery
    {
        public enum Type
        {
            READ, UPDATE
        }
        private YcsbQuery(Type type, object readKey, object payload)
        {
            this.type = type;
            this.readKey = readKey;
            this.payload = payload;
        }
        private Type type;
        private object readKey;
        private object payload;

        static public YcsbQuery GetRead(object key)
        {
            return new YcsbQuery(Type.READ, key, null);
        }
        static public YcsbQuery GetUpdate(object key, object payload)
        {
            return new YcsbQuery(Type.UPDATE, key, payload);
        }
        static public YcsbQuery FromType(
            Type type, object key, object payload = null)
        {
            switch (type)
            {
                case Type.READ: return GetRead(key);
                case Type.UPDATE: return GetUpdate(key, payload);
            }
            throw new ArgumentException("impossible");
        }
        internal TransactionExecution ExecuteIn(TransactionExecution txExec)
        {
            switch (this.type)
            {
                case Type.READ:
                    txExec.Read(this.readKey); break;
                case Type.UPDATE:
                    txExec.ReadThenUpdate(this.readKey, this.payload); break;
            }
            return txExec;
        }

        static internal YcsbQuery Generate(YCSBDataGenerator gen)
        {
            string nextop = gen.NextOperation();
            switch (nextop)
            {
                case "READ": return GetRead(gen.NextIntKey());
                case "UPDATE":
                    return GetUpdate(
         gen.NextIntKey(), YcsbHelper.UpdatePayload());
            }
            throw new Exception($"Unknown generated operation: {nextop}");
        }
    }

    class YcsbTx
    {
        public YcsbQuery[] queries;

        internal TransactionExecution ExecuteIn(TransactionExecution txExec)
        {
            txExec.Reset();
            for (int i = 0; i < queries.Length; ++i)
            {
                if (queries[i].ExecuteIn(txExec).IsAborted())
                {
                    return txExec;
                }
            }
            txExec.Commit();
            return txExec;
        }

        static internal YcsbTx Generate(int queryCount, YCSBDataGenerator gen)
        {
            YcsbTx tx = new YcsbTx
            {
                queries = new YcsbQuery[queryCount]
            };
            for (int i = 0; i < queryCount; ++i)
            {
                tx.queries[i] = YcsbQuery.Generate(gen);
            }
            return tx;
        }
    }

    class YcsbWorker : BenchmarkWorker
    {
        public class Output
        {
            public int NCommit = 0;
            public int NAbort = 0;
        }
        private List<YcsbTx> txs;
        private TransactionExecution txExec;
        public Output output;

        public override void Run()
        {
            for (int i = 0; i < txs.Count; ++i)
            {
                this.txs[i].ExecuteIn(this.txExec);
                if (txExec.IsAborted())
                {
                    ++this.abortCount;
                }
                else
                {
                    Debug.Assert(txExec.TxStatus == TxStatus.Committed);
                    ++this.commitCount;
                }
            }
            this.isFinished = true;
            this.output = new Output
            { NCommit = this.commitCount, NAbort = this.abortCount };
        }

        static internal YcsbWorker Generate(
            int txCount, int queryCount, YCSBDataGenerator gen, TransactionExecution txExec)
        {
            YcsbWorker workload = new YcsbWorker();
            workload.txs = new List<YcsbTx>(txCount);
            workload.txExec = txExec;
            for (int i = 0; i < txCount; ++i)
            {
                workload.txs.Add(YcsbTx.Generate(queryCount, gen));
            }
            return workload;
        }
    }

    class YcsbBenchmarkEnv
    {
        public YcsbBenchmarkEnv(
            VersionDb versionDb, Func<TransactionExecution, YcsbWorker> workerFactory)
        {
            var txExecs = SingletonYCSBTxExecHelper.MakeTxExecs(versionDb);
            this.workers = txExecs.AsParallel()
                .Select(workerFactory)
                .ToArray();
        }

        public SingletonYcsbBenchmark.BenchResult Go()
        {
            Thread[] threads = new Thread[this.workers.Length];
            GC.Collect();
            DateTime startTime = DateTime.UtcNow;
            for (int i = 0; i < this.workers.Length; ++i)
            {
                threads[i] = new Thread(this.workers[i].Run);
                threads[i].Start();
            }

            WorkerMonitor monitor = new WorkerMonitor(this.workers);
            monitor.StartBlocking(100);

            foreach (Thread t in threads)
            {
                t.Join();
            }
            DateTime endTime = DateTime.UtcNow;
            var result = YcsbBenchmarkEnv.CombineOutputs(
                this.workers.Select(w => w.output).ToArray(),
                endTime - startTime);
            result.SuggestedThroughput = monitor.SuggestThroughput();
            return result;
        }

        static private SingletonYcsbBenchmark.BenchResult CombineOutputs(
            YcsbWorker.Output[] partialOutputs, TimeSpan time)
        {
            var output = new SingletonYcsbBenchmark.BenchResult();
            output.CompleteTime = time;
            output.NCommit = partialOutputs.Select(_ => _.NCommit).Sum();
            output.NAbort = partialOutputs.Select(_ => _.NAbort).Sum();
            return output;
        }

        YcsbWorker[] workers;
    }

    internal class YcsbConfig
    {
        public int RecordCount = 500000;
        public int WorkerWorkload = 500000;
        public int Concurrency = 4;
        public int QueriesPerTx = 2;

        public Distribution Dist = Distribution.Zipf;
        public double ReadRatio = 0.5;
        public double ZipfSkew = 0.9;

        public void Print()
        {
            Console.WriteLine($"Workload per worker: {this.WorkerWorkload}");
            Console.WriteLine($"Concurrency: {this.Concurrency}");
            Console.WriteLine($"Query(s) in each tx: {this.QueriesPerTx}");
            Console.WriteLine($"Read ratio: {this.ReadRatio}");
            this.PrintDistribution();
            Console.WriteLine($"Workload per worker: {this.WorkerWorkload}");
            Console.WriteLine($"Records in db: {this.RecordCount}");
        }
        public void PrintSimple()
        {
            if (this.Dist == Distribution.Zipf) Console.Write($"Zipf(theta={this.ZipfSkew}), ");
            else Console.Write("Uniform, ");
            Console.WriteLine(String.Join(
                ", ", $"Read={this.ReadRatio}", $"Core={this.Concurrency}"));
        }
        private void PrintDistribution()
        {
            Console.Write("Distribution: ");
            if (this.Dist == Distribution.Uniform)
            {
                Console.WriteLine("Uniform");
            }
            else
            {
                Console.WriteLine($"Zipf, Skew: {this.ZipfSkew}");
            }
        }
    }

    public class SingletonYcsbBenchmark
    {
        public class BenchResult
        {
            public int NCommit = 0;
            public int NAbort = 0;
            public TimeSpan CompleteTime = new TimeSpan();
            public double Throughput
            {
                get
                {
                    return (NCommit + NAbort) / CompleteTime.TotalSeconds;
                }
            }
            public double AbortRate
            {
                get
                {
                    return NAbort / (NCommit + NAbort + 0.0);
                }
            }
            public int SuggestedThroughput = 0;

            public void Print()
            {
                Console.WriteLine($"commit: {this.NCommit}, abort: {this.NAbort}({this.AbortRate * 100:F3}%)");
                Console.WriteLine($"time: {this.CompleteTime.TotalSeconds:F2}, throughput: {this.Throughput:F2} txs/sec (monitor suggest: {this.SuggestedThroughput})");
            }

            static public BenchResult Average(IEnumerable<BenchResult> results)
            {
                int c = results.Count();
                BenchResult avg = new BenchResult();
                foreach (var result in results)
                {
                    avg.NAbort += result.NAbort;
                    avg.NCommit += result.NCommit;
                    avg.CompleteTime += result.CompleteTime;
                    avg.SuggestedThroughput += result.SuggestedThroughput;
                }
                avg.NAbort /= c;
                avg.NCommit /= c;
                avg.SuggestedThroughput /= c;
                avg.CompleteTime = new TimeSpan(avg.CompleteTime.Ticks / c);
                return avg;
            }
        }
        static void LoadYcsbData(SingletonVersionDb versionDb, int recordCount)
        {
            TransactionExecution txExec = versionDb.MakeTxExec();
            for (int i = 0; i < recordCount; ++i)
            {
                txExec.Reset();
                if (!txExec.Insert(i, YcsbHelper.NewPayload()).IsAborted())
                {
                    txExec.Commit();
                }
            }
        }
        static BenchResult BenchmarkWithConfigOnce(YcsbConfig config)
        {
            // config.Print();
            // Console.WriteLine();
            var versionDb = YcsbHelper.MakeVersionDb(config.Concurrency);
            // Console.Write("loading data... ");
            LoadYcsbData(versionDb, config.RecordCount);
            // Console.WriteLine("done");
            var generator = new YCSBDataGenerator(
                config.RecordCount, config.ReadRatio,
                config.Dist, config.ZipfSkew);
            Func<TransactionExecution, YcsbWorker> workerFactory =
                txExec => YcsbWorker.Generate(
                    config.WorkerWorkload, config.QueriesPerTx, generator, txExec);
            // Console.Write("generate workload... ");
            var benchmark = new YcsbBenchmarkEnv(versionDb, workerFactory);
            // Console.WriteLine("done");
            var result = benchmark.Go();
            SingletonVersionDb.DestroyInstance();
            return result;
        }

        static void PrintAverageResult(List<BenchResult> results)
        {
            if (results.Count == 1) return;
            Console.WriteLine("Average: ");
            BenchResult.Average(results).Print();
        }

        static void BenchmarkWithConfig(int repeat, YcsbConfig config)
        {
            List<BenchResult> results = new List<BenchResult>(repeat);
            for (int i = 0; i < repeat; ++i)
            {
                if (repeat > 1)
                {
                    Console.WriteLine($"ROUND {i + 1}:");
                }
                BenchResult result = BenchmarkWithConfigOnce(config);
                results.Add(result);
                result.Print();
            }
            PrintAverageResult(results);
            Console.WriteLine("---");
        }

        static void Pause()
        {
            Console.WriteLine("put any key to continue");
            Console.Read();
        }

        static void BenchmarkWithZipfConfigs(BenchmarkConfigs configs)
        {
            foreach (YcsbConfig config in configs.GetConfigs())
            {
                config.PrintSimple();
                BenchmarkWithConfig(configs.Repeat, config);
                GC.Collect();
            }
        }

        class BenchmarkConfigs
        {
            public int Repeat = 1;
            public Distribution Dist = Distribution.Zipf;
            public int RecordCount = 500000;
            public int WorkerWorkload = 500000;
            public int QueryPerTx = 2;
            public double[] ReadRatios = new double[] { 0, 0.5, 0.8, 1 };
            public double[] ZipfSkews = new double[] { 0.8, 0.9 };
            public int[] Concurrencies = new int[] { 1, 2, 4, 8, 16, 32 };

            static public BenchmarkConfigs Parse(string[] args)
            {
                BenchmarkConfigs config = new BenchmarkConfigs();
                for (int i = 0; i < args.Length; ++i)
                {
                    switch (args[i])
                    {
                        case "-u":
                        case "--uniform":
                            config.Dist = Distribution.Uniform;
                            break;
                        case "-r":
                        case "--records":
                            config.RecordCount = Convert.ToInt32(args[++i]);
                            break;
                        case "-w":
                        case "--workload":
                            config.WorkerWorkload = Convert.ToInt32(args[++i]);
                            break;
                        case "-c":
                        case "--concurrency":
                            config.Concurrencies = args[++i].Split(',').Select(s => Convert.ToInt32(s)).ToArray();
                            break;
                        case "-s":
                        case "--skew":
                            config.ZipfSkews = args[++i].Split(',').Select(s => Convert.ToDouble(s)).ToArray();
                            break;
                        case "-rr":
                        case "--read-ratio":
                            config.ReadRatios = args[++i].Split(',').Select(s => Convert.ToDouble(s)).ToArray();
                            break;
                        case "-q":
                        case "--query":
                            config.QueryPerTx = Convert.ToInt32(args[++i]);
                            break;
                        case "--repeat":
                            config.Repeat = Convert.ToInt32(args[++i]);
                            break;
                        default:
                            throw new ArgumentException($"unknown option {args[i]}");
                    }
                }
                return config;
            }

            public IEnumerable<YcsbConfig> GetConfigs()
            {
                YcsbConfig config = new YcsbConfig();
                config.Dist = this.Dist;
                config.RecordCount = this.RecordCount;
                config.WorkerWorkload = this.WorkerWorkload;
                config.QueriesPerTx = this.QueryPerTx;
                foreach (double skew in this.ZipfSkews)
                {
                    config.ZipfSkew = skew;
                    foreach (double rr in this.ReadRatios)
                    {
                        config.ReadRatio = rr;
                        foreach (int c in this.Concurrencies)
                        {
                            config.Concurrency = c;
                            yield return config;
                        }
                    }
                }
            }
        }

        public static void Main(string[] args)
        {
            BenchmarkConfigs configs = BenchmarkConfigs.Parse(args);
            BenchmarkWithZipfConfigs(configs);
            Pause();
        }
    }
}
