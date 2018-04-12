namespace TransactionBenchmarkTest.YCSB
{
    using GraphView.Transaction;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using ServiceStack.Redis;
    using System.IO;

    [TestClass]
    public abstract class YCSBTestBase
    {
        public static string INIT_DATA_FILE = "";

        [TestInitialize]
        public void Setup()
        {
            VersionDb versionDb = RedisVersionDb.Instance;

            // create version table
            versionDb.CreateVersionTable(SequenceRunner.TABLE_ID, SequenceRunner.REDIS_DB_INDEX);
            versionDb.CreateVersionTable(ConcurrencyRunner.TABLE_ID, ConcurrencyRunner.REDIS_DB_INDEX);

            Runner runner = new Runner(null);
            runner.TestMode = TestType.CorrectnessTest;

            // insert data
            using (StreamReader reader = new StreamReader(YCSBTestBase.INIT_DATA_FILE))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] fields = this.ParseLoadFormat(line);

                    // For sequenceRunner
                    Operation optr = new Operation(fields[0], SequenceRunner.TABLE_ID, fields[2], fields[3]);
                    runner.ExecuteOperation(optr);

                    // For concurrencyRunner
                    optr = new Operation(fields[0], ConcurrencyRunner.TABLE_ID, fields[2], fields[3]);
                    runner.ExecuteOperation(optr);
                }
            }
        }

        [TestCleanup]
        public void TestCleanUp()
        {
            VersionDb versionDb = RedisVersionDb.Instance;
            RedisClientManager clientManager = RedisClientManager.Instance;

            // delete data
            using (RedisClient client = clientManager.GetClient(ConcurrencyRunner.REDIS_DB_INDEX))
            {
                client.FlushDb();
            }
            using (RedisClient client = clientManager.GetClient(SequenceRunner.REDIS_DB_INDEX))
            {
                client.FlushDb();
            }

            // delete meta data
            versionDb.DeleteTable(SequenceRunner.TABLE_ID);
            versionDb.DeleteTable(ConcurrencyRunner.TABLE_ID);
        }

        private string[] ParseLoadFormat(string line)
        {
            return new string[] {
                // operation
                line.Substring(0, 6),
                // tableId
                line.Substring(7, 9),
                // key
                line.Substring(17, 23),
                // value
                line.Substring(50, 100)
            };
        }
    }
}
