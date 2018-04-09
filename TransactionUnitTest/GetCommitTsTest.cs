using GraphView.Transaction;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TransactionUnitTest
{
    [TestClass]
    public class GetCommitTsTest : AbstractTransactionTest
    {
        /// <summary>
        /// Test the function of the 'Choosing commit timestamp phase'
        /// </summary>
        [TestMethod]
        public void TestGetCommitTs()
        {
            //case1: read-only
            this.SetUp();
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(GetCommitTsTest.TABLE_ID, "key");
            bool isSuccess = t1.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(0, t1.CommitTs);

            //case2: update
            this.SetUp();
            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(GetCommitTsTest.TABLE_ID, "key");
            t2.Update(GetCommitTsTest.TABLE_ID, "key", "value1");
            t2.UploadLocalWriteRecords();
            isSuccess = t2.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(1, t2.CommitTs);

            //case3: read after other tx update
            this.SetUp();
            Transaction t3 = new Transaction(null, this.versionDb);
            t3.Read(GetCommitTsTest.TABLE_ID, "key");
            t3.Update(GetCommitTsTest.TABLE_ID, "key", "value1");
            t3.Commit();
            Transaction t4 = new Transaction(null, this.versionDb);
            t4.Read(GetCommitTsTest.TABLE_ID, "key");
            isSuccess = t4.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(1, t4.CommitTs);

            //case4: update after other tx update
            this.SetUp();
            Transaction t5 = new Transaction(null, this.versionDb);
            t5.Read(GetCommitTsTest.TABLE_ID, "key");
            t5.Update(GetCommitTsTest.TABLE_ID, "key", "value1");
            t5.Commit();
            Transaction t6 = new Transaction(null, this.versionDb);
            t6.Read(GetCommitTsTest.TABLE_ID, "key");
            t6.Update(GetCommitTsTest.TABLE_ID, "key", "value2");
            isSuccess = t6.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(2, t6.CommitTs);

            //case5: test the effect of tx.maxCommitTsOfWrites
            this.SetUp();
            this.versionDb.UpdateVersionMaxCommitTs(GetCommitTsTest.TABLE_ID, "key", 1, 5);
            Transaction t7 = new Transaction(null, this.versionDb);
            t7.Read(GetCommitTsTest.TABLE_ID, "key");
            t7.Update(GetCommitTsTest.TABLE_ID, "key", "value1");
            t7.UploadLocalWriteRecords();
            isSuccess = t7.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(6, t7.CommitTs);

            //case6: test the effect of tx.CommitLowerBound
            this.SetUp();
            Transaction t8 = new Transaction(null, this.versionDb);
            this.versionDb.UpdateCommitLowerBound(t8.TxId, 5);
            long t8CommitTs = this.versionDb.SetAndGetCommitTime(t8.TxId, 4);
            Assert.AreEqual(5, t8CommitTs);
        }
    }
}
