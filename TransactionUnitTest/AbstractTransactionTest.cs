using GraphView.Transaction;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceStack.Redis;
using System.Diagnostics;
using System.Threading;

namespace TransactionUnitTest
{
    [TestClass]
    public class AbstractTransactionTest
    {
        internal VersionDb versionDb;

        internal RedisClientManager clientManager;

        internal static readonly long TEST_REDIS_DB = 10L;

        internal static readonly string TABLE_ID = "unit_test_table";

        internal static readonly string DEFAULT_KEY = "key";

        internal static readonly string DEFAULT_VALUE = "value";

        [ClassInitialize]
        public static void ClassInit(TestContext context)
        {
            
        }

        [TestInitialize]
        public void TestInit()
        {
            // RedisVersionDb.Instance.PipelineMode = true;
            //this.versionDb = RedisVersionDb.Instance;
            //this.clientManager = ((RedisVersionDb)this.versionDb).RedisManager;

            //int partition = versionDb.PhysicalPartitionByKey(DEFAULT_KEY);
            //using (RedisClient redisClient = (RedisClient)this.clientManager.GetClient(TEST_REDIS_DB, partition))
            //{
            //    // 1. flush the test db
            //    redisClient.ChangeDb(TEST_REDIS_DB);
            //    redisClient.FlushDb();

            //    // 2. create version table, if table is null, it means the table with the same tableId has existed
            //    VersionTable table = this.versionDb.CreateVersionTable(TABLE_ID, TEST_REDIS_DB);

            //    // 3. load data
            //    Transaction tx = new Transaction(null, this.versionDb);
            //    tx.ReadAndInitialize(TABLE_ID, DEFAULT_KEY);
            //    tx.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
            //    tx.Commit();
            //}
            this.versionDb = CassandraVersionDb.Instance(4);
            VersionTable table = this.versionDb.CreateVersionTable(TABLE_ID, TEST_REDIS_DB);
            Transaction tx = new Transaction(null, this.versionDb);
            tx.ReadAndInitialize(TABLE_ID, DEFAULT_KEY);
            tx.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
            tx.Commit();
        }

        [TestCleanup]
        public void TestCleanup()
        {
           
        }

        [ClassCleanup]
        public void ClassCleanup()
        {

        }
    }
}

     
