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
        // dataset populating
        static void LoadTables(string baseDir)
        {
            Console.WriteLine("Loading tables...");
            RedisClient redisClient = new RedisClient(Constants.RedisHost, Constants.RedisPort);   // for payment to create `c_last` index
            redisClient.ChangeDb(Constants.RedisIndexDbN);
            redisClient.FlushAll(); // flush all first

            VersionDb redisVersionDb = RedisVersionDb.Instance();
            redisVersionDb.CreateVersionTable(Constants.DefaultTbl, Constants.RedisDbN);

            //string baseDir = Constants.BaseDirOfDatasets;
            string[] tables = Constants.TableNames;
            TableCode[] codes = Constants.TableCodes;

            long startTicks = DateTime.Now.Ticks;

            for (int i = 0; i < tables.Length; i++)
            {
                Console.WriteLine("Loading table " + tables[i]);
                var tablePath = baseDir + tables[i];
                var csvReader = new System.IO.StreamReader(tablePath);
                var code = codes[i];
                string line;
                var cnt = 0;
                int batchSize = 100;
                bool eatup = false;

                while(true)
                {
                    Transaction tx = new Transaction(null, redisVersionDb);
                    try
                    {
                        for (int k = 0; k < batchSize; k++)
                        {
                            line = csvReader.ReadLine();
                            if (line == null)
                            {
                                eatup = true;
                                break;
                            }
                            string[] columns = line.Split(Constants.Delimiter);
                            for (int j = 0; j < columns.Length; j++) { columns[j] = columns[j].Substring(1, columns[j].Length - 2); }   // remove head/tail `"` 

                            Tuple<string, string> kv = RecordGenerator.BuildRedisKV(code, columns, redisClient);

                            string tmpRecord = (string)tx.ReadAndInitialize(Constants.DefaultTbl, kv.Item1);
                            if (tmpRecord == null) tx.Insert(Constants.DefaultTbl, kv.Item1, kv.Item2);

                            cnt++;
                        }

                        tx.Commit();
                    } catch (TransactionException e) { }

                    if (cnt % 10000 == 0)
                    {
                        Console.WriteLine("\t Load records {0}", cnt);
                    }

                    if (eatup) break;
                }
            }

            long endTicks = DateTime.Now.Ticks;
            Console.WriteLine("Loading time total: {0} seconds", (endTicks - startTicks) / 10000000.0);
        }

        static void SyncLoadTpccTable(TpccTable table, VersionDb versionDb)
        {
            Console.WriteLine($"Start loading table: '{table.Type().Name()}'");
            var startTime = DateTime.UtcNow;

            int c = TPCCTableLoader.Load(
                Constants.BaseDirOfDatasets, table, versionDb);

            var time = (DateTime.UtcNow - startTime).TotalSeconds;

            Console.WriteLine(
                $"{c} records in '{table.Name()}' loaded in {time:F3} sec");
        }

        static void SyncLoadTpccTablesInto(VersionDb versionDb)
        {
            Parallel.ForEach(
                TpccTable.allTypes,
                t => SyncLoadTpccTable(TpccTable.Instance(t), versionDb));
        }

        static SingletonVersionDb MakeSingletonVersionDb()
        {
            Console.WriteLine("Initializing SingletonVersionDb");
            var versionDb = SingletonVersionDb.Instance(Constants.Singleton.Concurrency);
            versionDb.CreateVersionTable(Constants.DefaultTbl);
            return versionDb;
        }

        static void TPCCNewOrderTest(SyncExecutionBuilder execBuilder)
        {
            int workerCount = execBuilder.VersionDb.PartitionCount;
            int workloadCountPerWorker = 513364;
            Console.WriteLine("\nNEW-ORDER: w={0}, N={1}", workerCount, workloadCountPerWorker);

            TPCCBenchmark bench =
                new TPCCBenchmark(execBuilder, workloadCountPerWorker);
            bench.LoadNewOrderWorkload(Constants.NewOrderWorkloadPath);
            bench.Run();
            bench.PrintStats();

            Console.WriteLine("New-Order transaction throught: {0} tx/s", bench.Throughput);
        }

        static void TPCCPaymentTest(SyncExecutionBuilder execBuilder)
        {
            int workerCount = execBuilder.VersionDb.PartitionCount;
            int workloadCountPerWorker = 490443;
            Console.WriteLine("\nPAYMENT: w={0}, N={1}", workerCount, workloadCountPerWorker);

            TPCCBenchmark bench =
                new TPCCBenchmark(execBuilder, workloadCountPerWorker);
            bench.LoadPaymentWorkload(Constants.PaymentWorkloadPath);
            bench.Run();
            bench.PrintStats();

            Console.WriteLine("PAYMENT transaction throught: {0} tx/s", bench.Throughput);
        }

        static void SingletonTpccPaymentTest()
        {
            SingletonVersionDb versionDb = MakeSingletonVersionDb();
            var execBuilder = new SingletonExecutionBuilder(versionDb);
            SyncLoadTpccTablesInto(versionDb);
            TPCCPaymentTest(execBuilder);
        }
        static void SingletonTpccNewOrderBenchmark() {
            SingletonVersionDb versionDb = MakeSingletonVersionDb();
            var execBuilder = new SingletonExecutionBuilder(versionDb);
            SyncLoadTpccTablesInto(versionDb);
            TPCCNewOrderTest(execBuilder);
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
            SingletonTpccNewOrderBenchmark();
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
