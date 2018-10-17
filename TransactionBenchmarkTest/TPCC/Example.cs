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
            // foreach (var t in TpccTable.allTypes)
            // {
            //     SyncLoadTpccTable(TpccTable.Instance(t), versionDb, dir);
            // }
            Parallel.ForEach(
                TpccTable.AllUsedTypes,
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
                        var t = BenchmarkConfig.StringToTxType(args[i++]);
                        if (t == BenchmarkConfig.TransactionType.PAYMENT)
                        {
                            config.PaymentRatio = 1;
                        }
                        else
                        {
                            config.PaymentRatio = 0;
                        }
                        break;
                    case "-w":
                    case "--workload":
                        config.WorkloadPerWorker = Convert.ToInt32(args[i++]);
                        break;
                    case "-d":
                    case "--data-dir":
                        config.TpccFileDir = args[i++];
                        break;
                    case "-p":
                    case "--payment-ratio":
                        config.PaymentRatio = Convert.ToDouble(args[i++]);
                        break;
                    case "-W":
                    case "--warehouses":
                        config.Warehouses = Convert.ToInt32(args[i++]);
                        break;
                    default:
                        throw new ArgumentException($"unknown argument: {args[i - 1]}");
                }
            }
            config.TpccFileDir += $"-{config.Warehouses}";
        }

        static TPCCBenchmark InitializeBenchmark(
            SyncExecutionBuilder execBuilder,
            int workerWorkload, string workloadDir, double paymentRatio)
        {
            TPCCBenchmark benchmark = new TPCCBenchmark(execBuilder, workerWorkload);
            WorkloadLoader loader = new WorkloadLoader(workloadDir);
            WorkloadAllocator allocator = new HybridAllocator(loader, paymentRatio);
            benchmark.AllocateWorkload(allocator);
            return benchmark;
        }

        static void RunSyncBenchmark(TPCCBenchmark benchmark)
        {
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
            SyncLoadTpccTablesInto(
                versionDb, FileHelper.DataSetDir(config.TpccFileDir));

            var execBuilder = new SingletonExecutionBuilder(versionDb);
            var benchmark = InitializeBenchmark(
                execBuilder, config.WorkloadPerWorker,
                FileHelper.WorkloadDir(config.TpccFileDir),
                config.PaymentRatio);
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
