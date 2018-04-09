using GraphView.Transaction;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TransactionUnitTest
{
    /// <summary>
    /// Test the function of the 'Choosing commit timestamp phase'
    /// </summary>
    [TestClass]
    public class GetCommitTsTest : AbstractTransactionTest
    {
        [TestMethod]
        public void TestGetCommitTs1()
        {
            //case1: read-only
            this.SetUp();
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            bool isSuccess = t1.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(0L, t1.CommitTs);
        }

        [TestMethod]
        public void TestGetCommitTs2()
        {
            //case2: update
            this.SetUp();
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.UploadLocalWriteRecords();
            bool isSuccess = t1.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(1L, t1.CommitTs);
        }

        [TestMethod]
        public void TestGetCommitTs3()
        {
            //case3: read after other tx update
            this.SetUp();
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.Commit();
            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            bool isSuccess = t2.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(1L, t2.CommitTs);
        }

        [TestMethod]
        public void TestGetCommitTs4()
        {
            //case4: update after other tx update
            this.SetUp();
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.Commit();
            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            t2.Update(TABLE_ID, DEFAULT_KEY, "value2");
            bool isSuccess = t2.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(2L, t2.CommitTs);
        }

        [TestMethod]
        public void TestGetCommitTs5()
        {
            //case5: test the effect of tx.maxCommitTsOfWrites
            this.SetUp();
            this.versionDb.UpdateVersionMaxCommitTs(TABLE_ID, DEFAULT_KEY, 1L, 5L);
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.UploadLocalWriteRecords();
            bool isSuccess = t1.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(6L, t1.CommitTs);
        }

        [TestMethod]
        public void TestGetCommitTs6()
        {
            //case6: test the effect of tx.CommitLowerBound
            this.SetUp();
            Transaction t1 = new Transaction(null, this.versionDb);
            this.versionDb.UpdateCommitLowerBound(t1.TxId, 5L);
            long t1CommitTs = this.versionDb.SetAndGetCommitTime(t1.TxId, 4L);
            Assert.AreEqual(5L, t1CommitTs);
        }
    }
}
