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
                redisClient.ChangeDb(AbstractTransactionTest.TEST_REDIS_DB);
                redisClient.FlushDb();

                // 2. create version table
                this.versionDb.CreateVersionTable(AbstractTransactionTest.TABLE_ID, AbstractTransactionTest.TEST_REDIS_DB);

                // 3. load data
                Transaction tx = new Transaction(null, this.versionDb);
                tx.ReadAndInitialize(AbstractTransactionTest.TABLE_ID, AbstractTransactionTest.DEFAULT_KEY);
                tx.Insert(AbstractTransactionTest.TABLE_ID, AbstractTransactionTest.DEFAULT_KEY, 
                    AbstractTransactionTest.DEFAULT_VALUE);
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

     
