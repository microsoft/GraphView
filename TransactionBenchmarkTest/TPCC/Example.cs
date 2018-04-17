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
        static void LoadTables()
        {
            RedisClient redisClient = new RedisClient(Constants.RedisHost, Constants.RedisPort);   // for payment to create `c_last` index
            redisClient.ChangeDb(Constants.RedisIndexDbN);

            VersionDb redisVersionDb = RedisVersionDb.Instance;
            redisVersionDb.CreateVersionTable(Constants.DefaultTbl, Constants.RedisDbN);

            string baseDir = Constants.BaseDirOfDatasets;
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
                Console.WriteLine("Load {0} records", cnt);
            }

        }


        // Load New Order workload
        static Queue<NewOrderInParameters> LoadNewOrderWorkload()
        {
            Queue<NewOrderInParameters> queue = new Queue<NewOrderInParameters>();

            // load workload
            string filepath = Constants.NewOrderWorkloadPath;
            var csvReader = new System.IO.StreamReader(filepath);
            string line = null;
            line = csvReader.ReadLine();    // ignore the first line

            while ((line = csvReader.ReadLine()) != null)
            {
                string[] columns = line.Split(new string[] { Constants.WorkloadDelimiter }, StringSplitOptions.None);
                for (int j = 0; j < columns.Length; j++) { columns[j] = columns[j].Substring(1); }
                columns[columns.Length - 1] = columns[columns.Length - 1].Substring(0, columns[columns.Length - 1].Length - 1); // remove `"`

                var no = new NewOrderInParameters
                {
                    timestamp = columns[0],
                    W_ID = Convert.ToUInt32(columns[5]),
                    D_ID = Convert.ToUInt32(columns[3]),
                    C_ID = Convert.ToUInt32(columns[1]),
                    OL_I_IDs = JsonConvert.DeserializeObject<uint[]>(columns[6]),
                    OL_SUPPLY_W_IDs = JsonConvert.DeserializeObject<uint[]>(columns[4]),
                    OL_QUANTITYs = JsonConvert.DeserializeObject<uint[]>(columns[2]),
                    O_ENTRY_D = columns[7]
                };
                queue.Enqueue(no);
            }

            return queue;
        }

        // load payment workload
        static Queue<PaymentInParameters> LoadPaymentWorkload()
        {
            RedisClient redisClient = new RedisClient(Constants.RedisHost, Constants.RedisPort);   // for payment to access `c_last` index
            redisClient.ChangeDb(Constants.RedisIndexDbN);

            Queue<PaymentInParameters> queue = new Queue<PaymentInParameters>();

            // load workload
            string filepath = Constants.PaymentWorkloadPath;
            var csvReader = new System.IO.StreamReader(filepath);
            string line = null;
            line = csvReader.ReadLine();    // ignore the first line: header
            while ((line = csvReader.ReadLine()) != null)
            {
                string[] columns = line.Split(new string[] { Constants.WorkloadDelimiter }, StringSplitOptions.None);
                for (int j = 0; j < columns.Length; j++) { columns[j] = columns[j].Substring(1); }
                columns[columns.Length - 1] = columns[columns.Length - 1].Substring(0, columns[columns.Length - 1].Length - 1); // remove `"`

                var pm = new PaymentInParameters
                {
                    timestamp = columns[0],
                    C_ID = (columns[1] == "" ? 0 : Convert.ToUInt32(columns[1])),
                    C_LAST = columns[2],    // may be ""
                    H_DATE = columns[3],
                    C_D_ID = Convert.ToUInt32(columns[4]),
                    D_ID = Convert.ToUInt32(columns[5]),
                    W_ID = Convert.ToUInt32(columns[6]),
                    C_W_ID = Convert.ToUInt32(columns[7]),
                    H_AMOUNT = Convert.ToDouble(columns[8])
                };
                queue.Enqueue(pm);
            }

            return queue;
        }

        static void Main(string[] args)
        {
            // populate dataset
            Console.WriteLine("Loading TPCC tables...");
            LoadTables();

            // workload test
            VersionDb vdb = RedisVersionDb.Instance;
            vdb.CreateVersionTable(Constants.DefaultTbl, Constants.RedisDbN);
            RedisClient redisClient = new RedisClient(Constants.RedisHost, Constants.RedisPort);   // for payment to access `c_last` index
            redisClient.ChangeDb(Constants.RedisIndexDbN);

            var cntCommitted = 0;
            var cntAborted = 0;

            // New-Order
            Console.WriteLine("Loading New-Order workloads...");
            var noQueue = LoadNewOrderWorkload();

            Console.WriteLine("Doing New-Order");
            DateTime startDT = System.DateTime.Now;
            foreach (var e in noQueue)
            {
                var tpccwl = new TPCCNewOrderWorkload(e, vdb, null);
                var ret = (NewOrderOutput)tpccwl.Run();     // return to output
                if (ret.txFinalStatus == TxFinalStatus.COMMITTED)
                    cntCommitted++;
                else if (ret.txFinalStatus == TxFinalStatus.ABORTED)
                    cntAborted++;
            }
            DateTime endDT = System.DateTime.Now;
            TimeSpan ts = endDT.Subtract(startDT);

            Console.WriteLine("\t total:{0}, Commit:{1}, Abort:{2}", noQueue.Count, cntCommitted, cntAborted);
            Console.WriteLine("\t throughput:{0}, abort rate:{1}", (cntCommitted + cntAborted) / ts.TotalSeconds, cntAborted / (cntCommitted + cntAborted));
            Console.WriteLine();

            cntCommitted = 0;
            cntAborted = 0;

            // Payment
            Console.WriteLine("Loading Payment workloads...");
            var pmQueue = LoadPaymentWorkload();

            Console.WriteLine("Doing Payment");
            startDT = System.DateTime.Now;
            foreach (var e in pmQueue)
            {
                var tpccwl = new TPCCPaymentWorkload(e, vdb, redisClient);
                var ret = (PaymentOutput)tpccwl.Run();
                if (ret.txFinalStatus == TxFinalStatus.COMMITTED)
                    cntCommitted++;
                else if (ret.txFinalStatus == TxFinalStatus.ABORTED)
                    cntAborted++;
            }
            endDT = System.DateTime.Now;
            ts = endDT.Subtract(startDT);

            Console.WriteLine("\t total:{0}, Commit:{1}, Abort:{2}", pmQueue.Count, cntCommitted, cntAborted);
            Console.WriteLine("\t throughput:{0}, abort rate:{1}", (cntCommitted + cntAborted) / ts.TotalSeconds, cntAborted / (cntCommitted + cntAborted));
            Console.WriteLine();


            Console.WriteLine("Done.");
        }

    }



}
