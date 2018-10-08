using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.Transaction;
using Newtonsoft.Json;
using System.Collections;
using ServiceStack.Redis;

namespace TransactionBenchmarkTest.TPCC
{

    class Example
    {
        static void SyncLoadTpccTable(
            TpccTable table, VersionDb versionDb, string dir)
        {
            Console.WriteLine($"Start loading table: '{table.Type().Name()}'");
            var startTime = DateTime.UtcNow;

            int c = TPCCTableLoader.Load(dir, table, versionDb);

            var time = (DateTime.UtcNow - startTime).TotalSeconds;

            Console.WriteLine(
                $"{c} records in '{table.Name()}' loaded in {time:F3} sec");
        }

        static void SyncLoadTpccTablesInto(VersionDb versionDb, string dir)
        {
            Parallel.ForEach(
                TpccTable.allTypes,
                t => SyncLoadTpccTable(TpccTable.Instance(t), versionDb, dir));
        }

        static SingletonVersionDb MakeSingletonVersionDb(int concurrency)
        {
            Console.WriteLine("Initializing SingletonVersionDb");
            var versionDb = SingletonVersionDb.Instance(concurrency);
            return versionDb;
        }

        static void TPCCNewOrderAsyncTest()
        {
            int workerCount = 1;
            int workloadCountPerWorker = 1000;
            string workloadFile = "D:\\tpcc-txns\\NEW_ORDER.csv.shuffled";
            Console.WriteLine("\nNEW-ORDER: w={0}, N={1}", workerCount, workloadCountPerWorker);

            //TPCCStateTracer.nostates = new NewOrderState[workerCount*workloadCountPerWorker + 1]; // ignore the first line
            //TxAbortReasonTracer.reasons = new string[workerCount * workloadCountPerWorker + 1];

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>
            {
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(Constants.DefaultTbl, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                },
                //new List<Tuple<string, int>>()
                //{
                //    Tuple.Create(Constants.DefaultTbl, 0),
                //    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                //},
                //new List<Tuple<string, int>>()
                //{
                //    Tuple.Create(Constants.DefaultTbl, 0),
                //    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                //},
                //new List<Tuple<string, int>>()
                //{
                //    Tuple.Create(Constants.DefaultTbl, 0),
                //    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                //}

            };

            TPCCAsyncBenchmark bench = new TPCCAsyncBenchmark(workerCount, workloadCountPerWorker, instances);
            bench.LoadNewOrderWorkload(workloadFile);
            bench.Run("NewOrder");
            bench.Stats();


            //Console.WriteLine("Final state of each TX:");
            //for (int i = 1; i < TPCCStateTracer.nostates.Length; i++)
            //{
            //    Console.WriteLine("pid={0} \t NewOrderState={1}", i, TPCCStateTracer.nostates[i]);
            //}

            //Console.WriteLine("Abort reason of each tx:");
            //for (int i = 1; i < TxAbortReasonTracer.reasons.Length; i++)
            //{
            //    Console.WriteLine("pid={0} \t AbortReason={1}", i, TxAbortReasonTracer.reasons[i]);
            //}
        }

        static void TPCCPaymentAsyncTest()
        {
            int workerCount = 1;
            int workloadCountPerWorker = 1000;
            string workloadFile = "D:\\tpcc-txns\\PAYMENT.csv";
            Console.WriteLine("\nPAYMENT: w={0}, N={1}", workerCount, workloadCountPerWorker);

            //TPCCStateTracer.pmstates = new PaymentState[workerCount * workloadCountPerWorker + 1]; // ignore the first line
            //TxAbortReasonTracer.reasons = new string[workerCount * workloadCountPerWorker + 1];

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>
            {
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(Constants.DefaultTbl, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                },
                //new List<Tuple<string, int>>()
                //{
                //    Tuple.Create(Constants.DefaultTbl, 0),
                //    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                //},
                //new List<Tuple<string, int>>()
                //{
                //    Tuple.Create(Constants.DefaultTbl, 0),
                //    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                //},
                //new List<Tuple<string, int>>()
                //{
                //    Tuple.Create(Constants.DefaultTbl, 0),
                //    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                //}
            };

            TPCCAsyncBenchmark bench = new TPCCAsyncBenchmark(workerCount, workloadCountPerWorker, instances);
            bench.LoadPaymentWorkload(workloadFile);
            bench.Run("Payment");
            bench.Stats();

            //
            //Console.WriteLine("Final state of each TX:");
            //for (int i = 1; i < TPCCStateTracer.pmstates.Length; i++)
            //{
            //    Console.WriteLine("pid={0} \t PaymentState={1}", i, TPCCStateTracer.pmstates[i]);
            //}

            //Console.WriteLine("Abort reason of each tx:");
            //for (int i = 1; i < TxAbortReasonTracer.reasons.Length; i++)
            //{
            //    Console.WriteLine("pid={0} \t AbortReason={1}", i, TxAbortReasonTracer.reasons[i]);
            //}
        }

        static public void ParseArguments(string[] args)
        {
            BenchmarkConfig config = BenchmarkConfig.globalConfig;
            for (int i = 0; i < args.Length;)
            {
                switch (args[i++])
                {
                    case "-c":
                    case "--concurrency":
                        config.Concurrency = Convert.ToInt32(args[i++]);
                        break;
                    case "-t":
                    case "--type":
                        config.TxType = BenchmarkConfig.StringToTxType(args[i++]);
                        if (config.WorkloadFile == null)
                        {
                            config.WorkloadFile =
                                BenchmarkConfig.DefaultWorkloadFile(config.TxType);
                        }
                        break;
                    case "-w":
                    case "--workload":
                        config.WorkloadPerWorker = Convert.ToInt32(args[i++]);
                        break;
                    case "-d":
                    case "--data-dir":
                        config.DatasetDir = args[i++];
                        break;
                    case "-f":
                    case "--workload-file":
                        config.WorkloadFile = args[i++];
                        break;
                    default:
                        throw new ArgumentException($"unknown argument: {args[i - 1]}");
                }
            }
        }

        static WorkloadFactory GetWorkloadFactory(
            BenchmarkConfig.TransactionType txType)
        {
            switch (txType)
            {
                case BenchmarkConfig.TransactionType.PAYMENT:
                    return new PaymentWorkloadFactory();
                case BenchmarkConfig.TransactionType.NEW_ORDER:
                    return new NewOrderWorkloadFactory();
            }
            return null;
        }
        static TPCCBenchmark InitializeBenchmark(
            BenchmarkConfig.TransactionType txType,
            SyncExecutionBuilder execBuilder,
            int workerWorkload, string workloadFile)
        {
            TPCCBenchmark benchmark = new TPCCBenchmark(execBuilder, workerWorkload);
            benchmark.LoadWorkload(GetWorkloadFactory(txType), workloadFile);
            return benchmark;
        }

        static void RunSyncBenchmark(TPCCBenchmark benchmark) {
            benchmark.Run();
            benchmark.PrintStats();
            Console.WriteLine(
                "Transaction throughput: {0} tx/s", benchmark.Throughput);
        }

        static void SingletonTpccBenchmarkWithGlobalConfig()
        {
            BenchmarkConfig config = BenchmarkConfig.globalConfig;
            SingletonVersionDb versionDb =
                MakeSingletonVersionDb(config.Concurrency);
            SyncLoadTpccTablesInto(versionDb, config.DatasetDir);
            var execBuilder = new SingletonExecutionBuilder(versionDb);
            var benchmark = InitializeBenchmark(
                config.TxType, execBuilder,
                config.WorkloadPerWorker, config.WorkloadFile);
            RunSyncBenchmark(benchmark);
        }

        static void getchar(char c)
        {
            //Console.Beep(500, 600);
            Console.WriteLine("\n**** Please input char <{0}>.", c);
            while (true)
            {
                if (Console.Read() == c)
                    break;
            }
        }


        static void Main(string[] args)
        {
            ParseArguments(args);
            BenchmarkConfig.globalConfig.Print();
            SingletonTpccBenchmarkWithGlobalConfig();
            getchar('q');
            // LoadTables(Constants.BaseDirOfDatasets);

            // //TPCCNewOrderTest();
            // //TPCCPaymentTest();

            // //getchar('`');
            // for (int i = 0; i < 100; i++)
            // {
            //     TPCCNewOrderAsyncTest();
            //     Console.WriteLine();
            // }
            // //TPCCNewOrderAsyncTest();

            // //getchar('`');
            // //TPCCPaymentAsyncTest();

            // //Transaction tx = new Transaction(null, RedisVersionDb.Instance);
            // //var res = RedisVersionDb.Instance.GetVersionList("test", "D-2-1");
            // // Console.WriteLine("DONE");

            //getchar('`');
        }

    }



}
