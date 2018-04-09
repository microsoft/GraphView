using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView.Transaction;

namespace TransactionUnitTest
{
    [TestClass]
    public class UploadingUnitTest : AbstractTransactionTest
    {
        [TestMethod]
        // case1: upload new inserted entry successfully
        public void TestUploadNewCase1()
        {
            this.SetUp();

            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txDelete.Delete(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txDelete.Commit();

            Transaction txInsert = new Transaction(null, this.versionDb);
            txInsert.ReadAndInitialize(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txInsert.Insert(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY, "value_insert");
            bool success = txInsert.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
        }

        [TestMethod]
        // case2: upload new inserted entry failed
        public void TestUploadNewCase2()
        {
            this.SetUp();

            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txDelete.Delete(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txDelete.Commit();

            Transaction txInsert = new Transaction(null, this.versionDb);
            Transaction txInsert2 = new Transaction(null, this.versionDb);

            txInsert.ReadAndInitialize(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txInsert.Insert(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY, "value_insert");
            bool success = txInsert.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);

            txInsert2.ReadAndInitialize(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txInsert2.Insert(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY, "value_insert");
            success = txInsert.UploadLocalWriteRecords();
            Assert.AreEqual(success, false);
        }

        [TestMethod]
        // case1: read [Ts, inf, -1], and it's still [Ts, inf, -1] when replacing, no retry
        public void TestReplaceOldCase1()
        {
            this.SetUp();

            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txUpdate.Update(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY, "value_insert");
            bool success = txUpdate.UploadLocalWriteRecords();

            Assert.AreEqual(success, true);
        }

        [TestMethod]
        // case2: read [Ts, inf, -1], and it will be [Ts, inf, tx2 ] when replacing, tx is ongoing
        public void TestReplaceOldCase2()
        {
            this.SetUp();
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txDelete.Delete(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            bool success = txDelete.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);

            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txUpdate.Update(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY, "value_insert");
            success = txUpdate.UploadLocalWriteRecords();

            Assert.AreEqual(success, false);
        }

        [TestMethod]
        // case3: read [Ts, inf, -1], and it will be [Ts, inf, tx2] when replacing, tx is commited
        public void TestReplaceOldCase3()
        {
            this.SetUp();
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);

            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_insert");

            // Another transaction has been committed
            bool success = txDelete.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
            txDelete.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txDelete.TxId, TxStatus.Committed);

            success = txUpdate.UploadLocalWriteRecords();
            Assert.AreEqual(success, false);
        }

        [TestMethod]
        // case4: read [Ts, inf, -1], and it will be [Ts, inf, tx2] when replacing, tx is aborted
        public void TestReplaceOldCase4()
        {
            this.SetUp();
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txDelete.Delete(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            bool success = txDelete.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
            txDelete.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txDelete.TxId, TxStatus.Aborted);

            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txUpdate.Update(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY, "value_insert");
            success = txUpdate.UploadLocalWriteRecords();

            Assert.AreEqual(success, true);
        }

        [TestMethod]
        // case5: read [-1, -1, txId1], version entry is [-1, -1, txId1] when replacing
        public void TestReplaceOldCase5()
        {
            this.SetUp();
            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            bool success = txUpdate.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
            txUpdate.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);

            Transaction txUpdate2 = new Transaction(null, this.versionDb);
            txUpdate2.Read(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY);
            txUpdate2.Update(UploadingUnitTest.TABLE_ID, UploadingUnitTest.DEFAULT_KEY, "value_insert");
            success = txUpdate.UploadLocalWriteRecords();

            Assert.AreEqual(success, true);
        }
    }
}
