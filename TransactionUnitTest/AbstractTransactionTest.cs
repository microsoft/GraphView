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

     
