using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView.Transaction;

namespace TransactionUnitTest
{
    [TestClass]
    public class ValidationTest : AbstractTransactionTest
    {
        [TestMethod]
        public void TestValidation1()
        {
            //Test the function of UpdateVersionMaxCommitTs()
            VersionEntry versionEntry = this.versionDb.UpdateVersionMaxCommitTs(TABLE_ID, DEFAULT_KEY, 1L, 5L);
            Assert.AreEqual(5L, versionEntry.MaxCommitTs);

            versionEntry = this.versionDb.UpdateVersionMaxCommitTs(TABLE_ID, DEFAULT_KEY, 1L, 3L);
            Assert.AreEqual(5L, versionEntry.MaxCommitTs);
        }

		[TestMethod]
		public void TestValidation1Event()
		{
			//Test the function of UpdateVersionMaxCommitTs() in the event-driven senario.
			UpdateVersionMaxCommitTsRequest req = this.versionDb.EnqueueUpdateVersionMaxCommitTs(TABLE_ID, DEFAULT_KEY, 1L, 5L);
			while (req.Result == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
			}
			VersionEntry versionEntry = req.Result as VersionEntry;
			Assert.AreEqual(5L, versionEntry.MaxCommitTs);

			req = this.versionDb.EnqueueUpdateVersionMaxCommitTs(TABLE_ID, DEFAULT_KEY, 1L, 3L);
			while (req.Result == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
			}
			this.versionDb.Visit(TABLE_ID, 0);
			versionEntry = req.Result as VersionEntry;
			Assert.AreEqual(5L, versionEntry.MaxCommitTs);
		}

		[TestMethod]
        public void TestValidation2()
        {
            //Test the function of UpdateCommitLowerBound()
            Transaction t1 = new Transaction(null, this.versionDb);
            Assert.AreEqual(0L, this.versionDb.GetTxTableEntry(t1.TxId).CommitLowerBound);

            long t1CommitTs = this.versionDb.UpdateCommitLowerBound(t1.TxId, 5L);
            Assert.AreEqual(5L, this.versionDb.GetTxTableEntry(t1.TxId).CommitLowerBound);
            Assert.AreEqual(-1L, t1CommitTs);

            this.versionDb.SetAndGetCommitTime(t1.TxId, 0L);
            t1CommitTs = this.versionDb.UpdateCommitLowerBound(t1.TxId, 7L);
            Assert.AreEqual(5L, this.versionDb.GetTxTableEntry(t1.TxId).CommitLowerBound);
            Assert.AreEqual(5L, t1CommitTs);
        }

		[TestMethod]
		public void TestValidation2Event()
		{
			//Test the function of UpdateCommitLowerBound() in the event-driven senario.
			TransactionExecution tex1 = new TransactionExecution(null, this.versionDb);
			while (tex1.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex1.InitTx();
			}

			UpdateCommitLowerBoundRequest txCommitReq = this.versionDb.EnqueueUpdateCommitLowerBound(tex1.txId, 5L);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			long txCommitTs = txCommitReq.Result == null ? VersionDb.RETURN_ERROR_CODE : (long)txCommitReq.Result;
			GetTxEntryRequest getTxReq = this.versionDb.EnqueueGetTxEntry(tex1.txId);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(5L, txEntry.CommitLowerBound);
			Assert.AreEqual(-1L, txCommitTs);

			//Test the function of SetCommitTs() in the event-driven senario.
			SetCommitTsRequest setTsReq = this.versionDb.EnqueueSetCommitTs(tex1.txId, 6L);
			while (setTsReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			long commitTime = setTsReq.Result == null ? -1 : (long)setTsReq.Result;
			Assert.AreEqual(6L, commitTime);
		}

		[TestMethod]
        public void TestValidation3()
        {
            //the current version entry has not been held by any transaction
            //just perform range check
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.UploadLocalWriteRecords();
            this.versionDb.SetAndGetCommitTime(t1.TxId, 5L);
            t1.CommitTs = 5L;

            bool isSuccess = t1.Validate();
            Assert.AreEqual(true, isSuccess);

            VersionEntry versionEntry =
                this.versionDb.ReplaceVersionEntry(TABLE_ID, DEFAULT_KEY, 1L, 0L, 4L, -1, -1, long.MaxValue);
            isSuccess = t1.Validate();
            Assert.AreEqual(false, isSuccess);
        }

        [TestMethod]
        public void TestValidation4()
        {
            // The current version entry has been held by another concurrent transaction
            // And the concurrent transaction's status is Aborted

            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            t2.UploadLocalWriteRecords();

            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Delete(TABLE_ID, DEFAULT_KEY);
            t1.UploadLocalWriteRecords();
            this.versionDb.UpdateTxStatus(t1.TxId, TxStatus.Aborted);

            bool isSuccess = t2.Validate();
            Assert.AreEqual(true, isSuccess);
        }

        [TestMethod]
        public void TestValidation5()
        {
            // The current version entry has been held by another concurrent transaction
            // And the concurrent transaction's status is Committed

            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            t2.UploadLocalWriteRecords();

            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Delete(TABLE_ID, DEFAULT_KEY);
            t1.UploadLocalWriteRecords();
            this.versionDb.SetAndGetCommitTime(t1.TxId, 5L);
            t1.CommitTs = 5L;
            this.versionDb.UpdateTxStatus(t1.TxId, TxStatus.Committed);

            this.versionDb.SetAndGetCommitTime(t2.TxId, 4L);
            t2.CommitTs = 4L;
            bool isSuccess = t2.Validate();
            Assert.AreEqual(true, isSuccess);

            this.versionDb.SetAndGetCommitTime(t2.TxId, 6L);
            t2.CommitTs = 6L;
            isSuccess = t2.Validate();
            Assert.AreEqual(false, isSuccess);
        }

        [TestMethod]
        public void TestValidation6()
        {
            // The current version entry has been held by another concurrent transaction
            // if its status is Ongoing, try to push its commitLowerBound
            // the tx who is locking the version has not gotten its commitTs and I push its commitLowerBound successfully.

            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            t2.UploadLocalWriteRecords();

            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Delete(TABLE_ID, DEFAULT_KEY);
            t1.UploadLocalWriteRecords();

            this.versionDb.SetAndGetCommitTime(t2.TxId, 4L);
            t2.CommitTs = 4L;
            bool isSuccess = t2.Validate();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(5L, this.versionDb.GetTxTableEntry(t1.TxId).CommitLowerBound);
        }

        [TestMethod]
        public void TestValidation7()
        {
            // The current version entry has been held by another concurrent transaction
            // if its status is Ongoing, try to push its commitLowerBound
            // push failed, the transaction holding the version entry has gotten the commit time

            Transaction t2 = new Transaction(null, this.versionDb);
            t2.Read(TABLE_ID, DEFAULT_KEY);
            t2.UploadLocalWriteRecords();

            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Delete(TABLE_ID, DEFAULT_KEY);
            t1.UploadLocalWriteRecords();

            this.versionDb.SetAndGetCommitTime(t1.TxId, 5L);
            t1.CommitTs = 5L;

            this.versionDb.SetAndGetCommitTime(t2.TxId, 4L);
            t2.CommitTs = 4L;
            bool isSuccess = t2.Validate();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(0L, this.versionDb.GetTxTableEntry(t1.TxId).CommitLowerBound);

            this.versionDb.SetAndGetCommitTime(t2.TxId, 6L);
            t2.CommitTs = 6L;
            isSuccess = t2.Validate();
            Assert.AreEqual(false, isSuccess);
            Assert.AreEqual(0L, this.versionDb.GetTxTableEntry(t1.TxId).CommitLowerBound);
        }
    }
}
