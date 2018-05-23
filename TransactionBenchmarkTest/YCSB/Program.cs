using Cassandra;
using GraphView.Transaction;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

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
            test.rerun(1, 1000, operationFile);
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
            ISession session = cluster.Connect("msra");

            //var rs = session.Execute("INSERT INTO testapply2 (id, k, v) VALUES (3, 2, 3) IF NOT EXISTS");
            //var rs = session.Execute("INSERT INTO test4 (id, k, v, txid) VALUES (1, 'k1', 0x12, -1) IF NOT EXISTS");
            var rs = session.Execute("BEGIN BATCH UPDATE testapply set k=9,v=8 where id=1 if k<9 ;" +
                                                 "UPDATE testapply set v=8 where id=1 if v=11; APPLY BATCH");

            Console.WriteLine("--");
            var a = rs.GetEnumerator();
            a.MoveNext();
            var b = a.Current;
            var c = b.GetValue<bool>(0);



            //bool applied = rs.GetEnumerator().Current.GetValue<bool>(0);
            //Console.WriteLine("applied = {0}", applied);

            //foreach (var row in rs)
            //{
            //    Console.WriteLine("applied={0}", row.GetValue<bool>("[applied]"));
            //    //Console.WriteLine("applied={0}", row.GetValue<bool>(0));
            //}

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        static void YCSBAsyncTest()
        {
            const int partitionCount = 1;
            const int recordCount = 0;
            const int executorCount = 1;
            const int txCountPerExecutor = 1500000;
            //const bool daemonMode = true;
            const bool daemonMode = false;
            const string dataFile = "ycsb_data_lg_r.in";
            const string operationFile = "ycsb_ops_lg_r.in";

            // an executor is responsiable for all flush
            List<List<Tuple<string, int>>> instances = new List<List<Tuple<string, int>>>();

            TxResourceManager resourceManager = new TxResourceManager();

            // The default mode of versionDb is daemonMode
            //SingletonPartitionedVersionDb versionDb = SingletonPartitionedVersionDb.Instance(partitionCount, daemonMode);
            SingletonVersionDb versionDb = SingletonVersionDb.Instance(resourceManager);
            YCSBAsyncBenchmarkTest test = new YCSBAsyncBenchmarkTest(recordCount, 
                executorCount, txCountPerExecutor, versionDb, instances, resourceManager);

            test.Setup(dataFile, operationFile);
            test.Run();
            test.Stats();

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
            YCSBSyncTestWithCassandra();
            //test_cassandra();

            // For the redis benchmark Test
            // RedisBenchmarkTest();

            // For the YCSB async test
            //YCSBAsyncTest();

            // ExecuteRedisRawTest();
        }
    }
}
