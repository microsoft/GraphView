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
            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            bool success = txUpdate.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
            txUpdate.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);

            Transaction txUpdate2 = new Transaction(null, this.versionDb);
            object value = txUpdate2.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual((string)value, "value_update");

            txUpdate2.Update(TABLE_ID, DEFAULT_KEY, "value_update2");
            success = txUpdate2.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
        }

        [TestMethod]
        // case6: read [-1, -1, txId1], version entry is [Ts, Inf, -1] when replacing
        public void TestReplaceOldCase6()
        {
            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            bool success = txUpdate.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
            txUpdate.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);

            Transaction txUpdate2 = new Transaction(null, this.versionDb);
            object value = txUpdate2.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual((string)value, "value_update");

            txUpdate.PostProcessingAfterCommit();

            txUpdate2.Update(TABLE_ID, DEFAULT_KEY, "value_update2");
            success = txUpdate2.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
        }

        [TestMethod]
        // case7: read [Ts, inf, tx], version entry is [Ts, Inf, tx] when replacing tx is commited
        public void TestReplaceOldCase7()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            bool success = txDelete.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);

            Transaction txUpdate2 = new Transaction(null, this.versionDb);
            object value = txUpdate2.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual((string)value, "value");

            txDelete.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txDelete.TxId, TxStatus.Committed);

            txUpdate2.Update(TABLE_ID, DEFAULT_KEY, "value");
            success = txUpdate2.UploadLocalWriteRecords();
            Assert.AreEqual(success, false);
        }

        [TestMethod]
        // case8: read [Ts, inf, tx], version entry is [Ts, Inf, tx] when replacing tx is aborted
        public void TestReplaceOldCase8()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            bool success = txDelete.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);

            Transaction txUpdate2 = new Transaction(null, this.versionDb);
            object value = txUpdate2.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual((string)value, "value");

            txDelete.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txDelete.TxId, TxStatus.Aborted);

            txUpdate2.Update(TABLE_ID, DEFAULT_KEY, "value_update2");
            success = txUpdate2.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);
        }

        [TestMethod]
        // case9: read [Ts, inf, tx], version entry is [Ts, Ts', -1] when replacing
        public void TestReplaceOldCase9()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            bool success = txDelete.UploadLocalWriteRecords();
            Assert.AreEqual(success, true);

            Transaction txUpdate2 = new Transaction(null, this.versionDb);
            object value = txUpdate2.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual((string)value, "value");

            txDelete.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txDelete.TxId, TxStatus.Committed);
            txDelete.PostProcessingAfterCommit();

            txUpdate2.Update(TABLE_ID, DEFAULT_KEY, "value_update2");
            success = txUpdate2.UploadLocalWriteRecords();
            Assert.AreEqual(success, false);
        }
    }
}
