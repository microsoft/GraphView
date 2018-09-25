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
                case "UPDATE": return GetUpdate(gen.NextIntKey(), "TODO:");
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

    class YcsbWorkload
    {
        public class Output
        {
            public int NCommit = 0;
            public int NAbort = 0;
        }
        private List<YcsbTx> txs;

        internal Output ExecuteIn(TransactionExecution txExec)
        {
            Output output = new Output();
            for (int i = 0; i < txs.Count; ++i)
            {
                this.txs[i].ExecuteIn(txExec);
                if (txExec.IsAborted())
                {
                    ++output.NAbort;
                }
                else
                {
                    Debug.Assert(txExec.TxStatus == TxStatus.Committed);
                    ++output.NCommit;
                }
            }
            return output;
        }

        static internal YcsbWorkload Generate(
            int txCount, int queryCount, YCSBDataGenerator gen)
        {
            YcsbWorkload workload = new YcsbWorkload();
            workload.txs = new List<YcsbTx>(txCount);
            for (int i = 0; i < txCount; ++i)
            {
                workload.txs.Add(YcsbTx.Generate(queryCount, gen));
            }
            return workload;
        }
    }

    class YcsbBenchmarkEnv
    {
        public class LocalBenchmarkEnv
        {
            public LocalBenchmarkEnv(
                TransactionExecution txExec, YcsbWorkload workload)
            {
                this.txExec = txExec;
                this.workload = workload;
            }
            public void Start()
            {
                this.thread = new Thread(
                    () => this.output = this.workload.ExecuteIn(this.txExec));
                this.thread.Start();
            }
            public YcsbWorkload.Output GetOutput()
            {
                this.thread.Join();
                return this.output;
            }
            private TransactionExecution txExec;
            private YcsbWorkload workload;
            private YcsbWorkload.Output output;
            private Thread thread;
        }
        public YcsbBenchmarkEnv(
            VersionDb versionDb, Func<YcsbWorkload> workloadFactory)
        {
            var txExecs = SingletonYCSBTxExecHelper.MakeTxExecs(versionDb);
            this.localEnvs = txExecs.Select(
                exec => new LocalBenchmarkEnv(exec, workloadFactory())).ToArray();
        }

        public SingletonYcsbBenchmark.Output Go()
        {
            GC.Collect();
            DateTime startTime = DateTime.UtcNow;
            var outputs = new YcsbWorkload.Output[this.localEnvs.Length];
            for (int i = 0; i < this.localEnvs.Length; ++i)
            {
                this.localEnvs[i].Start();
            }
            for (int i = 0; i < this.localEnvs.Length; ++i)
            {
                outputs[i] = this.localEnvs[i].GetOutput();
            }
            DateTime endTime = DateTime.UtcNow;
            return YcsbBenchmarkEnv.CombineOutputs(
                outputs, endTime - startTime);
        }

        static private SingletonYcsbBenchmark.Output CombineOutputs(
            YcsbWorkload.Output[] partialOutputs, TimeSpan time)
        {
            var output = new SingletonYcsbBenchmark.Output();
            output.CompleteTime = time;
            output.NCommit = partialOutputs.Select(_ => _.NCommit).Sum();
            output.NAbort = partialOutputs.Select(_ => _.NAbort).Sum();
            return output;
        }

        LocalBenchmarkEnv[] localEnvs;
    }

    internal class YcsbConfig
    {
        public int RecordCount = 1000000;
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
            Console.WriteLine($"Workload per worker: {this.WorkerWorkload} ({this.WorkerWorkload * this.Concurrency} in total)");
            Console.WriteLine($"Records in db: {this.RecordCount}");
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
        public class Output
        {
            public int NCommit = 0;
            public int NAbort = 0;
            public TimeSpan CompleteTime;
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
            public void Print()
            {
                Console.WriteLine($"Commit Count: {this.NCommit}, Abort Count: {this.NAbort}");
                Console.WriteLine($"Abort Rate: {this.AbortRate * 100:F2}%");
                Console.WriteLine($"Throughput: {this.Throughput:F2} txs/sec");
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
        static void BenchmarkWithConfig(YcsbConfig config)
        {
            config.Print();
            Console.WriteLine();
            var versionDb = YcsbHelper.MakeVersionDb(config.Concurrency);
            LoadYcsbData(versionDb, config.RecordCount);
            var generator = new YCSBDataGenerator(
                config.RecordCount, config.ReadRatio,
                config.Dist, config.ZipfSkew);
            Func<YcsbWorkload> workloadFactory =
                () => YcsbWorkload.Generate(
                    config.WorkerWorkload, config.QueriesPerTx, generator);
            var benchmark = new YcsbBenchmarkEnv(versionDb, workloadFactory);
            var result = benchmark.Go();
            result.Print();
            Console.WriteLine();
        }


        public static void Main(string[] args)
        {
            YcsbConfig config = new YcsbConfig();
            BenchmarkWithConfig(config);
            Console.Read();
        }
    }
}
