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

            VersionDb redisVersionDb = RedisVersionDb.Instance;
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
                int batchSize = 10;
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
        
        static void TPCCNewOrderTest()
        {
            int workerCount = 2;
            int workloadCountPerWorker = 2000;
            string workloadFile = "D:\\tpcc-txns\\NEW_ORDER.csv";
            Console.WriteLine("\nNEW-ORDER: w={0}, N={1}", workerCount, workloadCountPerWorker);

            TPCCBenchmark bench = new TPCCBenchmark(workerCount, workloadCountPerWorker);
            bench.LoadNewOrderWorkload(workloadFile);
            bench.Run();

            Console.WriteLine("New-Order transaction throught: {0} tx/s", bench.Throughput);
        }

        static void TPCCPaymentTest()
        {
            int workerCount = 1;
            int workloadCountPerWorker = 2000;
            string workloadFile = "D:\\tpcc-txns\\PAYMENT.csv";
            Console.WriteLine("\nPAYMENT: w={0}, N={1}", workerCount, workloadCountPerWorker);

            TPCCBenchmark bench = new TPCCBenchmark(workerCount, workloadCountPerWorker);
            bench.LoadPaymentWorkload(workloadFile);
            bench.Run();

            Console.WriteLine("PAYMENT transaction throught: {0} tx/s", bench.Throughput);
        }


        static void TPCCNewOrderAsyncTest()
        {
            int workerCount = 1;
            int workloadCountPerWorker = 2000;
            string workloadFile = "D:\\tpcc-txns\\NEW_ORDER.csv";
            Console.WriteLine("\nNEW-ORDER: w={0}, N={1}", workerCount, workloadCountPerWorker);

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
        }

        static void TPCCPaymentAsyncTest()
        {
            int workerCount = 4;
            int workloadCountPerWorker = 10000;
            string workloadFile = "D:\\tpcc-txns\\PAYMENT.csv";
            Console.WriteLine("\nPAYMENT: w={0}, N={1}", workerCount, workloadCountPerWorker);

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>
            {
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(Constants.DefaultTbl, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                },
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(Constants.DefaultTbl, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                },
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(Constants.DefaultTbl, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                },
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(Constants.DefaultTbl, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                }
            };

            TPCCAsyncBenchmark bench = new TPCCAsyncBenchmark(workerCount, workloadCountPerWorker, instances);
            bench.LoadPaymentWorkload(workloadFile);
            bench.Run("Payment");
            bench.Stats();
        }


        static void Main(string[] args)
        {
            string baseDir = "D:\\tpcc-tables\\";
            //LoadTables(baseDir);

            //TPCCNewOrderTest();
            //TPCCPaymentTest();

            TPCCNewOrderAsyncTest();
            //TPCCPaymentAsyncTest();

            Console.WriteLine("DONE");
            while(true)
            {
                Console.Read();
            }            
        }

    }



}
