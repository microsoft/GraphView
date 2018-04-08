using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView.Transaction;

namespace TransactionUnitTest
{
    [TestClass]
    public class ExecutionTest
    {
        [TestMethod]
        public void TestMethod1()
        {
            Transaction tx0 = new Transaction(null, null);

            tx0.Insert("t1", 1, "bla");
            tx0.Insert("t1", 2, "bla");

            VersionDb redisDb = RedisVersionDb.Instance;

            Transaction tx1 = new Transaction(null, null);
            Transaction tx2 = new Transaction(null, null);

            object value = tx1.Read("t1", 1);

            tx2.Read("t1", 1);
            tx2.Update("t1", 1, "blabla");
            tx2.Commit();

            var versionList = redisDb.GetVersionList("t1", 1);
            long newVersionKey = -1;
            object value2 = tx1.GetVisibleVersionEntry(versionList, out newVersionKey);

            tx1.Commit();

            Debug.Assert(value2.Equals("blabla"));
        }
    }
}
