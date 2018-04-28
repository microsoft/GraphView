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
		public void TestPostProcessing1Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			texUpdate.Abort();
			while (texUpdate.CurrentProc != new Procedure(texUpdate.PostProcessingAfterAbort))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Abort();
			}
			texUpdate.PostProcessingAfterAbort();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.PostProcessingAfterAbort();
			}

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
		public void TestPostProcessing2Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			texUpdate.Abort();
			while (texUpdate.CurrentProc != new Procedure(texUpdate.PostProcessingAfterAbort))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Abort();
			}

			TransactionExecution texDelete = new TransactionExecution(null, this.versionDb);
			texDelete.DEBUG_MODE = true;
			while (texDelete.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.InitTx();
			}
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texDelete.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texDelete.Upload();
			while (texDelete.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.Upload();
			}

			texUpdate.PostProcessingAfterAbort();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.PostProcessingAfterAbort();
			}

			Assert.AreEqual(null, this.GetVersionByKey(2L));
			VersionEntry versionEntry = this.GetVersionByKey(1L);
			Assert.AreEqual(0L, versionEntry.BeginTimestamp);
			Assert.AreEqual(long.MaxValue, versionEntry.EndTimestamp);
			Assert.AreEqual(texDelete.txId, versionEntry.TxId);
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
		public void TestPostProcessing3Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.maxCommitTsOfWrites = 4L;
			texUpdate.Commit();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.CurrentProc();
			}

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

		[TestMethod]
		public void TestPostProcessing4Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.maxCommitTsOfWrites = 4L;
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Upload();
			}
			texUpdate.SetCommitTimestamp();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			texUpdate.Validate();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.Validate))
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Validate();
			}
			Assert.AreEqual(new Procedure(texUpdate.WriteToLog), texUpdate.CurrentProc);
			texUpdate.WriteToLog();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.WriteToLog))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.WriteToLog();
			}

			TransactionExecution texDelete = new TransactionExecution(null, this.versionDb);
			texDelete.DEBUG_MODE = true;
			while (texDelete.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.InitTx();
			}
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texDelete.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texDelete.Upload();
			while (texDelete.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.Upload();
			}

			texUpdate.PostProcessingAfterCommit();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.PostProcessingAfterCommit();
			}

			VersionEntry newVersionEntry = this.GetVersionByKey(2L);
			Assert.AreEqual(5L, newVersionEntry.BeginTimestamp);
			Assert.AreEqual(long.MaxValue, newVersionEntry.EndTimestamp);
			Assert.AreEqual(texDelete.txId, newVersionEntry.TxId);

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
