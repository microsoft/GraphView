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

            for (int i = 0; i < tables.Length; i++)
            {
                Console.WriteLine("Loading table " + tables[i]);
                var tablePath = baseDir + tables[i];
                var csvReader = new System.IO.StreamReader(tablePath);
                var code = codes[i];
                string line;
                var cnt = 0;
                while ((line = csvReader.ReadLine()) != null)
                {
                    string[] columns = line.Split(Constants.Delimiter);
                    for (int j = 0; j < columns.Length; j++) { columns[j] = columns[j].Substring(1, columns[j].Length - 2); }   // remove head/tail `"` 

                    Tuple<string, string> kv = RecordGenerator.BuildRedisKV(code, columns, redisClient);

                    // tx
                    Transaction tx = new Transaction(null, redisVersionDb);
                    try
                    {
                        string tmpRecord = (string)tx.ReadAndInitialize(Constants.DefaultTbl, kv.Item1);
                        if (tmpRecord == null) tx.Insert(Constants.DefaultTbl, kv.Item1, kv.Item2);
                        tx.Commit();
                    }
                    catch (TransactionException e) { }

                    cnt++;
                    if (cnt % 10000 == 0) Console.WriteLine("\tcnt={0}", cnt);
                }
                Console.WriteLine("\tLoad {0} records", cnt);
            }

        }
        
        static void TPCCNewOrderTest()
        {
            int workerCount = 4;
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
            int workerCount = 4;
            int workloadCountPerWorker = 2000;
            string workloadFile = "D:\\tpcc-txns\\PAYMENT.csv";
            Console.WriteLine("\nPAYMENT: w={0}, N={1}", workerCount, workloadCountPerWorker);

            TPCCBenchmark bench = new TPCCBenchmark(workerCount, workloadCountPerWorker);
            bench.LoadPaymentWorkload(workloadFile);
            bench.Run();

            Console.WriteLine("PAYMENT transaction throught: {0} tx/s", bench.Throughput);
        }

        static void Main(string[] args)
        {
            //string baseDir = "D:\\tpcc-tables\\";
            //LoadTables(baseDir);

            TPCCNewOrderTest();

            //TPCCPaymentTest();

            Console.WriteLine("DONE");
            Console.Read();
        }

    }



}
