using GraphView.Transaction;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceStack.Redis;

namespace TransactionUnitTest
{
    [TestClass]
    public class AbstractTransactionTest
    {
        internal VersionDb versionDb;

        internal IRedisClientsManager clientManager;

        internal static readonly long TEST_REDIS_DB = 10L;

        internal static readonly string TABLE_ID = "unit_test_table";

        internal static readonly string DEFAULT_KEY = "key";

        internal static readonly string DEFAULT_VALUE = "value";
        /// <summary>
        /// Define our own setup methods
        /// </summary>
        internal void SetUp()
        {
            using (RedisClient redisClient = (RedisClient)this.clientManager.GetClient())
            {
                // 1. flush the test db
                redisClient.ChangeDb(TEST_REDIS_DB);
                redisClient.FlushDb();

                // 2. create version table, if table is null, it means the table with the same tableId has existed
                VersionTable table = this.versionDb.CreateVersionTable(TABLE_ID, TEST_REDIS_DB);
                
                // 3. load data
                Transaction tx = new Transaction(null, this.versionDb);
                tx.ReadAndInitialize(TABLE_ID, DEFAULT_KEY);
                tx.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
                tx.Commit();
            }
        }

        [ClassInitialize()]
        public static void ClassInit(TestContext context)
        {

        }

        [TestInitialize()]
        public void Initialize()
        {
            this.versionDb = RedisVersionDb.Instance;
            this.clientManager = RedisClientManager.Instance;
        }

        [TestCleanup()]
        public void Cleanup()
        {

        }
    }
}

     
