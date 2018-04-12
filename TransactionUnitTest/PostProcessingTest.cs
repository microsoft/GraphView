using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView.Transaction;

namespace TransactionUnitTest
{
    [TestClass]
    public class PostProcessingTest : AbstractTransactionTest
    {
        [TestMethod]
        public void TestPostProcessing1()
        {
            // Abort transaction
            // new version should be removed
            // old version shoud be rolled back from [Ts, inf, myTxId] to [Ts, inf, -1], if no other transaction is locking it.
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.UploadLocalWriteRecords();
            t1.PostProcessingAfterAbort();

            Assert.AreEqual(null, this.GetVersionByKey(2L));

            VersionEntry versionEntry = this.GetVersionByKey(1L);
            Assert.AreEqual(0L, versionEntry.BeginTimestamp);
            Assert.AreEqual(long.MaxValue, versionEntry.EndTimestamp);
            Assert.AreEqual(-1L, versionEntry.TxId);
        }

        [TestMethod]
        public void TestPostProcessing2()
        {
            // Abort transaction
            // new version should be removed
            // try to roll back the old version from [Ts, inf, myTxId] to [Ts, inf, -1], 
            // but other transaction is locking it, do nothing

            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.UploadLocalWriteRecords();
            this.versionDb.UpdateTxStatus(t1.TxId, TxStatus.Aborted);

            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            t2.Delete(TABLE_ID, DEFAULT_KEY);
            t2.UploadLocalWriteRecords();

            t1.PostProcessingAfterAbort();

            Assert.AreEqual(null, this.GetVersionByKey(2L));

            VersionEntry versionEntry = this.GetVersionByKey(1L);
            Assert.AreEqual(0L, versionEntry.BeginTimestamp);
            Assert.AreEqual(long.MaxValue, versionEntry.EndTimestamp);
            Assert.AreEqual(t2.TxId, versionEntry.TxId);
        }

        [TestMethod]
        public void TestPostProcessing3()
        {
            // Commit transaction
            // new version should be updated, if no other transaction is locking it.
            // old version shoud be updated from [Ts, inf, myTxId] to [Ts, myCommitTs, -1]

            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.UploadLocalWriteRecords();
            this.versionDb.SetAndGetCommitTime(t1.TxId, 5L);
            t1.CommitTs = 5L;
            this.versionDb.UpdateTxStatus(t1.TxId, TxStatus.Committed);
            t1.PostProcessingAfterCommit();

            VersionEntry newVersionEntry = this.GetVersionByKey(2L);
            Assert.AreEqual(5L, newVersionEntry.BeginTimestamp);
            Assert.AreEqual(long.MaxValue, newVersionEntry.EndTimestamp);
            Assert.AreEqual(-1L, newVersionEntry.TxId);

            VersionEntry oldVersionEntry = this.GetVersionByKey(1L);
            Assert.AreEqual(0L, oldVersionEntry.BeginTimestamp);
            Assert.AreEqual(5L, oldVersionEntry.EndTimestamp);
            Assert.AreEqual(-1L, oldVersionEntry.TxId);
        }

        [TestMethod]
        public void TestPostProcessing4()
        {
            // Commit transaction
            // new version should be updated, but other transaction is locking it, do nothing
            // old version shoud be updated from [Ts, inf, myTxId] to [Ts, myCommitTs, -1]

            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.UploadLocalWriteRecords();
            this.versionDb.SetAndGetCommitTime(t1.TxId, 5L);
            t1.CommitTs = 5L;
            this.versionDb.UpdateTxStatus(t1.TxId, TxStatus.Committed);

            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            t2.Delete(TABLE_ID, DEFAULT_KEY);
            t2.UploadLocalWriteRecords();

            t1.PostProcessingAfterCommit();

            VersionEntry newVersionEntry = this.GetVersionByKey(2L);
            Assert.AreEqual(5L, newVersionEntry.BeginTimestamp);
            Assert.AreEqual(long.MaxValue, newVersionEntry.EndTimestamp);
            Assert.AreEqual(t2.TxId, newVersionEntry.TxId);

            VersionEntry oldVersionEntry = this.GetVersionByKey(1L);
            Assert.AreEqual(0L, oldVersionEntry.BeginTimestamp);
            Assert.AreEqual(5L, oldVersionEntry.EndTimestamp);
            Assert.AreEqual(-1L, oldVersionEntry.TxId);
        }

        private VersionEntry GetVersionByKey(long versionKey)
        {
            IEnumerable<VersionEntry> versionList = this.versionDb.GetVersionList(TABLE_ID, DEFAULT_KEY);
            foreach (VersionEntry versionEntry in versionList)
            {
                if (versionEntry.VersionKey == versionKey)
                {
                    return versionEntry;
                }
            }

            return null;
        }
    }
}
