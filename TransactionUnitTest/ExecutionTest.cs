using System.Collections.Generic;
using GraphView.Transaction;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;

namespace TransactionUnitTest
{
    [TestClass]
    public class ExecutionTest : AbstractTransactionTest
    {
        [TestMethod]
        public void TestReadCase1()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case1: Initial Read
            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, DEFAULT_VALUE);
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase2()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case2: Read after local update
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, DEFAULT_VALUE);
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase3()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case3: Read after uploading
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            txUpdate.UploadLocalWriteRecords();
            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, DEFAULT_VALUE);
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase4()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case4: Read after getting commitTime
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, DEFAULT_VALUE);
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase5()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case5: Read after validation
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            txUpdate.Validate();
            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, DEFAULT_VALUE);
            Assert.AreEqual(largestVersionKey, 1L);

        }

        [TestMethod]
        public void TestReadCase6()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case6: Read after commit
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            txUpdate.Validate();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);

            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, "value_update");
            Assert.AreEqual(largestVersionKey, 2L);

        }

        [TestMethod]
        public void TestReadCase7()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case7:Read after postprofessing
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            txUpdate.Validate();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);
            txUpdate.PostProcessingAfterCommit();

            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, "value_update");
            Assert.AreEqual(largestVersionKey, 2L);
        }

        [TestMethod]
        public void TestReadCase8()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case8: Read after abort
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Aborted);

            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, DEFAULT_VALUE);
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        public void TestReadCase9()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case8: Read after abort postprocessing
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            txUpdate.UploadLocalWriteRecords();
            txUpdate.GetCommitTimestamp();
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Aborted);
            txUpdate.PostProcessingAfterAbort();

            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual((string)value, DEFAULT_VALUE);
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        // Read [Ts, Ts', -1]
        public void TestReadCase10()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txDelete = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case8: Read after delete
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Commit();

            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual(value, null);
            Assert.AreEqual(largestVersionKey, 1L);
        }

        [TestMethod]
        // Read after insert
        public void TestReadCase11()
        {
            Transaction txInsert = new Transaction(null, this.versionDb);
            txInsert.Read(TABLE_ID, DEFAULT_KEY);
            txInsert.Delete(TABLE_ID, DEFAULT_KEY);
            txInsert.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
            string value = (string) txInsert.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual(value, "value_insert");
        }

        [TestMethod]
        // Read after delete
        public void TestReadCase12()
        {
            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Delete(TABLE_ID, DEFAULT_KEY);
            string value = (string)txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual(value, null);
        }

        [TestMethod]
        // Read after update
        public void TestReadCase13()
        {
            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
            string value = (string) txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual(value, "value_update");
        }

        [TestMethod]
        // can not insert
        [ExpectedException(typeof(TransactionException))]
        public void TestInsertCase1()
        {
            Transaction txInsert = new Transaction(null, this.versionDb);
            txInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY);
            txInsert.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
        }

        [TestMethod]
        // delete -> insert
        public void TestInsertCase2()
        {
            Transaction txInsert = new Transaction(null, this.versionDb);
            txInsert.Read(TABLE_ID, DEFAULT_KEY);
            txInsert.Delete(TABLE_ID, DEFAULT_KEY);
            txInsert.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE + "_insert");
            txInsert.Commit();

            string value = (string)txInsert.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual(value, DEFAULT_VALUE + "_insert");
        }

        [TestMethod]
        // update -> insert
        [ExpectedException(typeof(TransactionException))]
        public void TestInsertCase3()
        {
            Transaction txInsert = new Transaction(null, this.versionDb);
            txInsert.Read(TABLE_ID, DEFAULT_KEY);
            txInsert.Update(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
            txInsert.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
        }

        [TestMethod]
        // update
        public void TestUpdateCase1()
        {
            Transaction txUpdate = new Transaction(null, this.versionDb);
            txUpdate.Read(TABLE_ID, DEFAULT_KEY);
            txUpdate.Update(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE + "_update");
            txUpdate.Commit();

            Transaction txRead = new Transaction(null, this.versionDb);
            string value = (string) txRead.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual(value, DEFAULT_VALUE + "_update");
        }

        [TestMethod]
        // delete -> update
        [ExpectedException(typeof(TransactionException))]
        public void TestUpdateCase2()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Update(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE + "_insert");
        }

        [TestMethod]
        // delete
        public void TestDeleteCase1()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Commit();

            Transaction txRead = new Transaction(null, this.versionDb);
            string value = (string)txRead.Read(TABLE_ID, DEFAULT_KEY);
            Assert.AreEqual(value, null);
        }

        [TestMethod]
        // insert -> delete
        public void TestDeleteCase2()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE + "_insert");
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Commit();
        }

        [TestMethod]
        // delete -> delete
        [ExpectedException(typeof(TransactionException))]
        public void TestDeleteCase3()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
        }

        [TestMethod]
        // update -> delete
        public void TestDeleteCase4()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Update(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE + "_update");
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
        }

        private object ReadValue(Transaction tx, string tableId, object recordKey, out long largestVersionKey)
        {
            IEnumerable<VersionEntry> versionList = this.versionDb.GetVersionList(TABLE_ID, DEFAULT_KEY);
            VersionEntry version = tx.GetVisibleVersionEntry(versionList, out largestVersionKey);
            if (version == null)
            {
                return null;
            }
            return version.Record;
        }
    }
}
