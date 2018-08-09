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
        public static void YCSBSyncTestWithPartitionedCassandra(string[] args)
        {
            int workerCount = 1;
            int taskCountPerWorker = 1000;
            string dataFile = "ycsb_data_r.in";
            string operationFile = "ycsb_ops_r.in";
            
            // Cassandra version db
            int maxVdbCnt = 8192;
            List<VersionDb> vdbList = new List<VersionDb>();
            for (int j = 0; j < maxVdbCnt; j++)
            {
                vdbList.Add(PartitionedCassandraVersionDb.Instance(1, "127.0.0.1", 1, ConsistencyLevel.One));
            }
            YCSBBenchmarkTest test = new YCSBBenchmarkTest(0, 0, vdbList[0]);

            test.LoadDataWithMultiThreads(dataFile, vdbList, 10);


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

            //test.rerun(2048, 125, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(4096, 50, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //test.rerun(8192, 25, operationFile, vdbList);
            //Console.WriteLine("*****************************************************");

            //Console.WriteLine("done");
            //Console.ReadLine();
        }


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
            
            test.LoadDataWithMultiThreads(dataFile, vdbList, 1);


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

        public static void test_cassandra2()
        {
            Cluster cluster = Cluster.Builder().AddContactPoints(new string[] { "127.0.0.1" }).Build();
            ISession session = cluster.Connect("msra");
            RowSet rs3 = session.Execute("update t set v1=3 where k=1 if v1>3");
            RowSet rs4 = session.Execute("update t set v1=3 where k=4 if v2=3");
            Console.WriteLine("hh");
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

            //void runCQL(int s, int e) // [s, e)
            //{
            //    for (int ii = s; ii < e; ii += cycle)
            //    {
            //        var rs = session.Execute(cmd_arr[ii]);
            //    }
            //}

            //void runCycle(int tid, int s, int e) // [s, e)
            //{
            //    for (int ii = s; ii < e; ii++)
            //    {
            //        //var rs = session.Execute(cql_statements[ii]);
            //        //var rs = sess_arr[tid].Execute(cql_statements[ii]);
            //        var rs = session.ExecuteAsync(cql_statements[ii]);
            //        //var statement = new SimpleStatement(cmd_arr[ii]);                    
            //        //var rs = sess_arr[tid].Execute(statement);
            //        //var rs = sess_arr[tid].ExecuteAsync(cmd_arr[ii]);
            //    }
            //}





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
                    //Thread thread = new Thread(() => runCycle(j, s, e));
                    //threadList.Add(thread);
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
            //List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>
            //{
            //    new List<Tuple<string, int>>()
            //    {
            //        Tuple.Create(VersionDb.TX_TABLE, 0),
            //        Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
            //    },
            //    new List<Tuple<string, int>>()
            //    {
            //        Tuple.Create(VersionDb.TX_TABLE, 0),
            //        Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
            //    },
            //    new List<Tuple<string, int>>()
            //    {
            //        Tuple.Create(VersionDb.TX_TABLE, 0),
            //        Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
            //    },
            //    new List<Tuple<string, int>>()
            //    {
            //        Tuple.Create(VersionDb.TX_TABLE, 0),
            //        Tuple.Create(YCSBAsyncBenchmarkTest.TABLE_ID, 0)
            //    },
            //};
            string[] tables = new string[]
            {
                VersionDb.TX_TABLE,
                YCSBAsyncBenchmarkTest.TABLE_ID
            };

            //TxResourceManager resourceManager = new TxResourceManager();

            // The default mode of versionDb is daemonMode
            CassandraVersionDb versionDb = CassandraVersionDb.Instance(partitionCount, 0);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                executorCount, txCountPerExecutor, versionDb, tables);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();

            Console.WriteLine("done");
            Console.ReadLine();

        }


        public static void YCSBAsyncTestWithPartitionedCassandra()
        {
            const int partitionCount = 4;
            const int recordCount = 100;
            const int executorCount = 4;
            const int txCountPerExecutor = 250;

            const string dataFile = "ycsb_data_r.in";
            const string operationFile = "ycsb_ops_r.in";
                        
            string[] tables = new string[]
            {
                VersionDb.TX_TABLE,
                YCSBAsyncBenchmarkTest.TABLE_ID
            };
            
            // The default mode of versionDb is daemonMode
            PartitionedCassandraVersionDb versionDb = PartitionedCassandraVersionDb.Instance(partitionCount, "127.0.0.1", 1, ConsistencyLevel.One);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                executorCount, txCountPerExecutor, versionDb, tables);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();            
        }

        static void LoadDataWithSyncForCassandra(string filename, int nThread, VersionDb vdb)
        {
            List<VersionDb> vdbList = new List<VersionDb>();
            for (int i=0; i<nThread; i++)
            {
                vdbList.Add(vdb);
            }

            YCSBBenchmarkTest sync_test = new YCSBBenchmarkTest(0, 0, vdb);
            sync_test.LoadDataWithMultiThreads(filename, vdbList, nThread);
        }
        public static void YCSBAsyncTestWithPartitionedCassandraHybrid(string[] args)
        {
            string action = "run";
            int workerCount = 500;
            int taskCountPerWorker = 1000;
            int partitionCount = 500;    // equal to workerCount

            int runall = 1;
            int stableRoundStart = 5;
            int stableRoundEnd = 15;

            /// one data file
            string dataFile = "ycsb_data_u.in";

            /// four kinds of operation(workload) files
            string operationFile = "ycsb_ops_u.in";

            /// <summary>
            /// CassandraVersionDb Single Node parameters
            /// </summary>
            string contactPoints = "127.0.0.1";
            int replicationFactor = 1;
            ConsistencyLevel consistencyLevel = ConsistencyLevel.One;

            /// <summary>
            /// CassandraVersionDb Cluster parameters - 10 nodes
            /// </summary>
            //string contactPoints = "10.6.0.4,10.6.0.5,10.6.0.6,10.6.0.12,10.6.0.13,10.6.0.14,10.6.0.15,10.6.0.16,10.6.0.17,10.6.0.18";
            //int replicationFactor = 3;
            //ConsistencyLevel consistencyLevel = ConsistencyLevel.Quorum;

            // see YCSBAsyncBenchmarkTest.run2 to know all types
            //string exeType = "ycsb_sync_ro_strk";
            //string exeType = "ycsb_sync_ro_intk"
            //string exeType = "ycsb_sync_wo_intk";
            //string exeType = "ycsb_sync_rw_intk";
            string exeType = "async";
            //string exeType = "cassandra_ro_intk";
            //string exeType = "cassandra_wo_intk";

            int i = 0;
            while (i < args.Length)
            {
                switch (args[i++])
                {
                    case "--datafile":
                        dataFile = args[i++];
                        break;
                    case "--opsfile":   // unused 
                        operationFile = args[i++];
                        break;
                    case "--workers":
                        workerCount = int.Parse(args[i++]);
                        break;
                    case "--taskspw":
                        taskCountPerWorker = int.Parse(args[i++]);
                        break;
                    case "--partitions":    // equal to workers
                        partitionCount = int.Parse(args[i++]);
                        break;
                    case "--nodes":
                        contactPoints = args[i++];
                        break;
                    case "--replica":       // default 3
                        replicationFactor = int.Parse(args[i++]);
                        break;
                    case "--consislevel":   // local: one, cluster: quorum
                        if (args[i++] == "one")
                        {
                            consistencyLevel = ConsistencyLevel.One;
                        } else
                        {
                            consistencyLevel = ConsistencyLevel.Quorum;
                        }
                        break;
                    case "--exetype":       // 
                        exeType = args[i++];
                        break;
                    case "--startround":    // unused
                        stableRoundStart = int.Parse(args[i++]);
                        break;
                    case "--endround":      // unused
                        stableRoundEnd = int.Parse(args[i++]);
                        break;
                    case "--runall":        // unused
                        runall = int.Parse(args[i++]);
                        break;
                    case "--action":        // load/run
                        action = args[i++];
                        break;
                    default:
                        break;
                }
            }

            Console.WriteLine("contact points " + contactPoints);
            if (runall == 0 && stableRoundEnd <= stableRoundStart)
            {
                Console.WriteLine("Bad stable round setting");
                return;
            }

            if (action == "load")
            {
                PartitionedCassandraVersionDb versionDb = PartitionedCassandraVersionDb.Instance(partitionCount, contactPoints, replicationFactor, consistencyLevel);
                LoadDataWithSyncForCassandra(dataFile, workerCount, versionDb);
            } else if (action == "run")
            {
                string[] tables = new string[]
                {
                    VersionDb.TX_TABLE,
                    YCSBAsyncBenchmarkTest.TABLE_ID
                };

                // The default mode of versionDb is daemonMode
                PartitionedCassandraVersionDb versionDb = PartitionedCassandraVersionDb.Instance(partitionCount, contactPoints, replicationFactor, consistencyLevel);
                YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(0,
                    workerCount, taskCountPerWorker, versionDb, tables);
                if (runall == 0)
                {
                    test.SetStableRound(stableRoundStart, stableRoundEnd);
                }

                // sync test
                //test.SetupOpsNull();

                // async test
                test.SetupOps(operationFile);
                test.StartMonitors();

                test.Run2(exeType);
                test.Stats2();
            }
            else
            {
                Console.WriteLine("bad action. Only <load> or <run> allowed");
            }
        }
     
        public static void Main(string[] args)
        {
            //long a = 10000;
            //Console.WriteLine(a.GetHashCode());
            //Console.WriteLine(a.ToString().GetHashCode());

            //Interlocked.
            //YCSBSyncTestWithCassandra(args);
            //YCSBSyncTestWithPartitionedCassandra(args);

            YCSBAsyncTestWithPartitionedCassandraHybrid(args);

            //YCSBAsyncTestWithPartitionedCassandra();
            //test_cassandra2();

            Console.WriteLine("Done");
            Console.ReadLine();
            //Console.ReadLine();
            //Console.ReadLine();
            //Console.ReadLine();
        }
    }

}
