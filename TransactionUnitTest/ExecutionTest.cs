using System.Collections.Generic;
using GraphView.Transaction;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TransactionUnitTest
{
    [TestClass]
    public class ExecutionTest : AbstractTransactionTest
    {
        [TestMethod]
        public void TestReadCase1()
        {
            this.SetUp();
            Transaction txRead = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case1: Initial Read
            object value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase2()
        {
            this.SetUp();
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case2: Read after local update
            txUpdate.Read(ExecutionTest.TABLE_ID, "key");
            txUpdate.Update(ExecutionTest.TABLE_ID, "key", "value_update");
            object value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase3()
        {
            this.SetUp();
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case3: Read after uploading
            txUpdate.Read(ExecutionTest.TABLE_ID, "key");
            txUpdate.Update(ExecutionTest.TABLE_ID, "key", "value_update");
            txUpdate.UploadLocalWriteRecords();
            object value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase4()
        {
            this.SetUp();
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case4: Read after getting commitTime
            txUpdate.Read(ExecutionTest.TABLE_ID, "key");
            txUpdate.Update(ExecutionTest.TABLE_ID, "key", "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            object value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase5()
        {
            this.SetUp();
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case5: Read after validation
            txUpdate.Read(ExecutionTest.TABLE_ID, "key");
            txUpdate.Update(ExecutionTest.TABLE_ID, "key", "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            txUpdate.Validate();
            object value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);

        }

        [TestMethod]
        public void TestReadCase6()
        {
            this.SetUp();
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case6: Read after commit
            txUpdate.Read(ExecutionTest.TABLE_ID, "key");
            txUpdate.Update(ExecutionTest.TABLE_ID, "key", "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            txUpdate.Validate();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);

            object value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value_update");
            Assert.AreEqual(largestVersionKey, 2L);

        }

        [TestMethod]
        public void TestReadCase7()
        {
            this.SetUp();
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case6: Read after commit
            txUpdate.Read(ExecutionTest.TABLE_ID, "key");
            txUpdate.Update(ExecutionTest.TABLE_ID, "key", "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            txUpdate.Validate();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);
            txUpdate.PostProcessingAfterCommit();

            object value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value_update");
            Assert.AreEqual(largestVersionKey, 2L);
        }

        private object ReadValue(Transaction tx, string tableId, object recordKey, out long largestVersionKey)
        {
            IEnumerable<VersionEntry> versionList = this.versionDb.GetVersionList(ExecutionTest.TABLE_ID, "key");
            VersionEntry version = tx.GetVisibleVersionEntry(versionList, out largestVersionKey);
            if (version == null)
            {
                return null;
            }
            return version.Record;
        }
    }
}
