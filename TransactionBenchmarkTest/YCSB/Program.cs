using Cassandra;
using GraphView.Transaction;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using TransactionBenchmarkTest.TPCC;

namespace TransactionBenchmarkTest.YCSB
{
    class Program
    {

        private static string[] args;

        static void ExecuteRedisRawTest()
        {
            RedisRawTest.BATCHES = 25000;
            RedisRawTest.REDIS_INSTANCES = 4;

            // ONLY FOR SEPARATE PROGRESS
            RedisRawTest.REDIS_INSTANCES = 1;
            //Console.Write("Input the Redis Id (start from 1): ");
            //string line = Console.ReadLine();
            string line = args[0];
            int redisId = int.Parse(line);
            RedisRawTest.OFFSET = redisId - 1;


            new RedisRawTest().Test();

            Console.Write("Type Enter to close...");
            Console.Read();
        }

        static void RedisBenchmarkTest()
        {
            const int workerCount = 4;
            const int taskCount = 1000000;
            const bool pipelineMode = true;
            const int pipelineSize = 100;

            RedisBenchmarkTest test = new RedisBenchmarkTest(workerCount, taskCount, pipelineMode, pipelineSize);
            test.Setup();
            test.Run();
            test.Stats();
        }

        /// <summary>
        /// For YCSB sync benchmark test
        /// </summary>
        static void YCSBTest()
        {
            const int workerCount = 4;    // 4;
            const int taskCountPerWorker = 2000000;   // 50000;
            const string dataFile = "ycsb_data_lg_r.in";
            const string operationFile = "ycsb_ops_lg_r.in";

			// REDIS VERSION DB
			// VersionDb versionDb = RedisVersionDb.Instance;
			// SINGLETON VERSION DB
			VersionDb versionDb = SingletonVersionDb.Instance();

            YCSBBenchmarkTest test = new YCSBBenchmarkTest(workerCount, taskCountPerWorker, versionDb);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();
        }

        static void YCSBSyncTestWithCassandra()
        {
            //const int workerCount = 1;    // 4;
            //const int taskCountPerWorker = 2000;   // 50000;

            const string dataFile = "ycsb_data_r.in";
            const string operationFile = "ycsb_ops_r.in";

            // Cassandra version db
            VersionDb versionDb = CassandraVersionDb.Instance();
            YCSBBenchmarkTest test = new YCSBBenchmarkTest(0, 0, versionDb);
            //test.LoadData(dataFile);

            Console.WriteLine("++++++++ Before");
            CassandraSessionManager.CqlCountShow();

            test.rerun(1, 50000, operationFile);
            Console.WriteLine("*****************************************************");

            Console.WriteLine("++++++++ After");
            CassandraSessionManager.CqlCountShow();



            //test.rerun(1, 2000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(1, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(2, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(4, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(6, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(8, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(10, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(20, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(50, 10000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(100, 5000, operationFile);
            //Console.WriteLine("*****************************************************");

            //test.rerun(200, 2500, operationFile);
            //Console.WriteLine("*****************************************************");


            Console.WriteLine("done");
            Console.ReadLine();
        }

        static void test_cassandra()
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
            for (int i=0; i<cnt; i++)
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
                for (int ii=s; ii<e; ii+=cycle)
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
                    var rs =  session.ExecuteAsync(cql_statements[ii]);
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

        static void YCSBAsyncTestWithCassandra()
        {
            const int partitionCount = 1;
            const int recordCount = 10000;
            const int executorCount = 1;
            const int txCountPerExecutor = 10000;

            const string dataFile = "ycsb_data_r.in";
            const string operationFile = "ycsb_ops_r.in";

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>
            {
                new List<Tuple<string, int>>()
                {
                    Tuple.Create(Constants.DefaultTbl, 0),
                    Tuple.Create(RedisVersionDb.TX_TABLE, 0),
                },
            };

            //TxResourceManager resourceManager = new TxResourceManager();

            // The default mode of versionDb is daemonMode
            CassandraVersionDb versionDb = CassandraVersionDb.Instance(partitionCount);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                executorCount, txCountPerExecutor, versionDb);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();

        }

        static void YCSBAsyncTest()
        {
            const int partitionCount = 3;
            const int recordCount = 0;
            const int executorCount = partitionCount;
            const int txCountPerExecutor = 1000000;
            //const bool daemonMode = true;
            const bool daemonMode = false;
            const string dataFile = "ycsb_data_lg_r.in";
            const string operationFile = "ycsb_ops_lg_r.in";
            YCSBAsyncBenchmarkTest.RESHUFFLE = false;


            // an executor is responsiable for all flush
            string[] tables = 
            {
                YCSBAsyncBenchmarkTest.TABLE_ID,
                VersionDb.TX_TABLE
            };

            // The default mode of versionDb is daemonMode
            SingletonPartitionedVersionDb versionDb = SingletonPartitionedVersionDb.Instance(partitionCount, daemonMode);
            // SingletonVersionDb versionDb = SingletonVersionDb.Instance(executorCount);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount,
                executorCount, txCountPerExecutor, versionDb, tables);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();

            Console.WriteLine("Enqueued Requests: {0}", SingletonPartitionedVersionDb.EnqueuedRequests);
            //versionDb.Active = false;
        }

        internal static void PinThreadOnCores()
        {
            Thread.BeginThreadAffinity();
            Process Proc = Process.GetCurrentProcess();
            foreach (ProcessThread pthread in Proc.Threads)
            {
                if (pthread.Id == AppDomain.GetCurrentThreadId())
                {
                    long AffinityMask = (long)Proc.ProcessorAffinity;
                    AffinityMask &= 0x0010;
                    // AffinityMask &= 0x007F;
                    pthread.ProcessorAffinity = (IntPtr)AffinityMask;
                }
            }

            Thread.EndThreadAffinity();
        }

        public static void Main(string[] args)
        {
            Program.args = args;
            // For the YCSB sync test
            // YCSBTest();
            // YCSBSyncTestWithCassandra();
            // test_cassandra();

            // For the redis benchmark Test
            // RedisBenchmarkTest();

            // For the YCSB async test
            YCSBAsyncTest();
            //YCSBAsyncTestWithCassandra();

            // ExecuteRedisRawTest();
        }
    }
}
