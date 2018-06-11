using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.Transaction;
using Cassandra;
using System.IO;
using System.Threading;
using ServiceStack.Redis;

namespace TransactionBenchmarkTest.YCSB
{
    class Example
    {
        public static void YCSBSyncTestWithCassandra(string[] args)
        {
            string[] contactPoints = { "127.0.0.1" };
            int replicationFactor = 1;
            int workerCount = 1;
            int taskCountPerWorker = 1000;
            string dataFile = "ycsb_data_r.in";
            string operationFile = "ycsb_ops_r.in";

            int i = 1;
            while(i < args.Length)
            {
                switch(args[i++])
                {
                    case "--contacts":
                        contactPoints = args[i++].Split(',');
                        break;
                    case "--repfactor":
                        replicationFactor = int.Parse(args[i++]);
                        break;
                    case "--workers":
                        workerCount = int.Parse(args[i++]);
                        break;
                    case "--tasks":
                        taskCountPerWorker = int.Parse(args[i++]);
                        break;
                    case "--data":
                        dataFile = args[i++];
                        break;
                    case "--ops":
                        operationFile = args[i++];
                        break;
                    default:
                        break;
                }
            }

            //Console.WriteLine("contact points: " + contactPoints.Length);
            //Console.WriteLine("replica: " + replicationFactor);
            //Console.WriteLine("worker: " + workerCount);
            //Console.WriteLine("taskPerWorker: " + taskCountPerWorker);
            //Console.WriteLine("data: " + dataFile);
            //Console.WriteLine("ops: " + operationFile);
            //return;


            // Cassandra version db
            int maxVdbCnt = 8192;
            List<VersionDb> vdbList = new List<VersionDb>();
            for(int j=0; j<maxVdbCnt; j++)
            {
                vdbList.Add(CassandraVersionDb.Instance(1, j));
            }
            YCSBBenchmarkTest test = new YCSBBenchmarkTest(0, 0, vdbList[0]);
            
            //test.LoadDataWithMultiThreads(dataFile, vdbList, 1);


            // run
            //test.rerun(1, 5000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");
            
            //test.rerun(2, 5000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(4, 5000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(8, 5000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(16, 5000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(32, 5000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(64, 5000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(128, 2000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(64, 2000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(256, 1000, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(512, 500, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(1024, 250, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            test.rerun(2048, 125, operationFile, vdbList);
            Console.WriteLine("*****************************************************");

            test.rerun(4096, 50, operationFile, vdbList);
            Console.WriteLine("*****************************************************");

            test.rerun(8192, 25, operationFile, vdbList);
            Console.WriteLine("*****************************************************");

            //Console.WriteLine("done");
            //Console.ReadLine();
        }

        public static void test_cassandra()
        {
            Cluster cluster = Cluster.Builder().AddContactPoints(new string[] { "127.0.0.1" }).Build();
            //ISession session = cluster.Connect("msra");
            ISession session = cluster.Connect("versiondb");

            string cmd_file = "cql1-50000.txt";
            string[] cmd_arr = new string[300000];
            StreamReader reader = new StreamReader(cmd_file);
            int cnt = 0;
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                cmd_arr[cnt++] = line;
            }
            Console.WriteLine("cmd count: {0}", cnt);

            SimpleStatement[] cql_statements = new SimpleStatement[cnt];
            for (int i = 0; i < cnt; i++)
            {
                cql_statements[i] = new SimpleStatement(cmd_arr[i]);
            }

            //Console.ReadLine();
            Console.WriteLine("start running...");

            int cycle = 6;

            //int[] num_t_arr = new int[] { 4, 8, 20, 50, };
            int[] num_t_arr = new int[] { 4, };

            ISession[] sess_arr = new ISession[50];
            for (int i = 0; i < 50; i++)
            {
                sess_arr[i] = cluster.Connect("versiondb");
            }

            void runCQL(int s, int e) // [s, e)
            {
                for (int ii = s; ii < e; ii += cycle)
                {
                    var rs = session.Execute(cmd_arr[ii]);
                }
            }

            void runCycle(int tid, int s, int e) // [s, e)
            {
                for (int ii = s; ii < e; ii++)
                {
                    //var rs = session.Execute(cql_statements[ii]);
                    //var rs = sess_arr[tid].Execute(cql_statements[ii]);
                    var rs = session.ExecuteAsync(cql_statements[ii]);
                    //var statement = new SimpleStatement(cmd_arr[ii]);                    
                    //var rs = sess_arr[tid].Execute(statement);
                    //var rs = sess_arr[tid].ExecuteAsync(cmd_arr[ii]);
                }
            }





            // run each CQL
            //foreach (int num_t in num_t_arr)
            //{
            //    Console.WriteLine("{0} THREADs @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@", num_t);
            //    session.Execute("TRUNCATE TABLE tx_table");


            //    int num_per_t = cnt / num_t;

            //    for (int k = 0; k < cycle; k++)
            //    {
            //        Console.WriteLine("running cql {0} with {1} thread*********************", k, num_t);
            //        // 
            //        List<Thread> threadList = new List<Thread>();
            //        for (int j = 0; j < num_t; j++)
            //        {
            //            int s = j * num_per_t + k;
            //            int e = (j + 1) * num_per_t + k;
            //            Thread thread = new Thread(() => runCQL(s, e));
            //            threadList.Add(thread);
            //        }

            //        // start
            //        long start = DateTime.Now.Ticks;
            //        foreach (Thread thread in threadList)
            //        {
            //            thread.Start();
            //        }
            //        foreach (Thread thread in threadList)
            //        {
            //            thread.Join();
            //        }
            //        long end = DateTime.Now.Ticks;

            //        // compute throughput
            //        double delta = (end - start) * 1.0 / 10000000;  // seconds
            //        double throughput = (cnt / 6) / delta;

            //        Console.WriteLine("cql type {0}, {1} thread, throughput = {2}/s", k, num_t, throughput);
            //    }
            //}

            Console.WriteLine();
            Console.WriteLine("Runing cycles using threads...");

            // run cycle
            foreach (int num_t in num_t_arr)
            {
                Console.WriteLine("{0} THREADs %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%", num_t);
                session.Execute("TRUNCATE TABLE tx_table");

                int num_per_t = cnt / num_t;

                // build thread
                List<Thread> threadList = new List<Thread>();
                for (int j = 0; j < num_t; j++)
                {
                    int s = j * num_per_t;
                    int e = (j + 1) * num_per_t;
                    Thread thread = new Thread(() => runCycle(j, s, e));
                    threadList.Add(thread);
                }

                // start
                long start = DateTime.Now.Ticks;
                foreach (Thread thread in threadList)
                {
                    thread.Start();
                }
                foreach (Thread thread in threadList)
                {
                    thread.Join();
                }
                long end = DateTime.Now.Ticks;

                // compute throughput
                double delta = (end - start) * 1.0 / 10000000;  // seconds
                double throughput = cnt / delta;

                Console.WriteLine("run CYCLE with {0} thread, throughput = {1}/s", num_t, throughput);
            }

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        public static void YCSBAsyncTestWithCassandra()
        {
            const int partitionCount = 20;
            const int recordCount = 500000;
            const int executorCount = 20;
            const int txCountPerExecutor = 25000;

            const string dataFile = "ycsb_data_r.in";
            const string operationFile = "ycsb_ops_r.in";

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>
            {
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(VersionDb.TX_TABLE, 0),
                    Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
                },
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(VersionDb.TX_TABLE, 0),
                    Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
                },
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(VersionDb.TX_TABLE, 0),
                    Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
                },
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(VersionDb.TX_TABLE, 0),
                    Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
                },
            };

            //TxResourceManager resourceManager = new TxResourceManager();

            // The default mode of versionDb is daemonMode
            CassandraVersionDb versionDb = CassandraVersionDb.Instance(partitionCount, 0);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                executorCount, txCountPerExecutor, versionDb, instances);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();

            Console.WriteLine("done");
            Console.ReadLine();

        }

        public static void Main(string[] args)
        {
            string destination = args.Length > 0 ? args[0] : "cassandra";
            switch(destination)
            {
                case "cassandra":
                    YCSBSyncTestWithCassandra(args);
                    break;
                default:
                    YCSBSyncTestWithCassandra(args);
                    break;
            }

            Console.WriteLine("Done");
            //Console.ReadLine();
        }   
    }
}
