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
        public void TestSetGetCommitTs1()
        {
            //case1: read-only
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            bool isSuccess = t1.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(0L, t1.CommitTs);
        }

		[TestMethod]
		public void TestSetGetCommitTs1Event()
		{
			TransactionExecution tex = new TransactionExecution(null, this.versionDb);
			while (tex.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.InitTx();
			}
			tex.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				tex.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(tex.txId);
            this.versionDb.EnqueueTxEntryRequest(tex.txId, getTxReq);

			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(0L, txEntry.CommitTime);
		}

        [TestMethod]
        public void TestSetGetCommitTs2()
        {
            //case2: update
            Transaction t1 = new Transaction(null, this.versionDb);
            t1.Read(TABLE_ID, DEFAULT_KEY);
            t1.Update(TABLE_ID, DEFAULT_KEY, "value1");
            t1.UploadLocalWriteRecords();
            bool isSuccess = t1.GetCommitTimestamp();
            Assert.AreEqual(true, isSuccess);
            Assert.AreEqual(1L, t1.CommitTs);
        }

		[TestMethod]
		public void TestSetGetCommitTs2Event()
		{
			TransactionExecution tex = new TransactionExecution(null, this.versionDb);
			while (tex.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.InitTx();
			}
			tex.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				tex.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			tex.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(tex.txId);
            this.versionDb.EnqueueTxEntryRequest(tex.txId, getTxReq);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(1L, txEntry.CommitTime);
		}

		[TestMethod]
        public void TestSetGetCommitTs3()
        {
            //case3: read after other tx update
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
		public void TestSetGetCommitTs3Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedUpdate, out object payloadUpdate);
			while (!receivedUpdate)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedUpdate, out payloadUpdate);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Commit();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.CurrentProc();
			}

			TransactionExecution tex = new TransactionExecution(null, this.versionDb);
			while (tex.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.InitTx();
			}
			tex.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				tex.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(tex.txId);
            this.versionDb.EnqueueTxEntryRequest(tex.txId, getTxReq);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(1L, txEntry.CommitTime);
		}

		[TestMethod]
        public void TestSetGetCommitTs4()
        {
            //case4: update after other tx update
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
		public void TestSetGetCommitTs4Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedUpdate, out object payloadUpdate);
			while (!receivedUpdate)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedUpdate, out payloadUpdate);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Commit();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.CurrentProc();
			}

			TransactionExecution tex = new TransactionExecution(null, this.versionDb);
			while (tex.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.InitTx();
			}
			tex.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				tex.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			tex.Update(TABLE_ID, DEFAULT_KEY, "value_update_1");
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(tex.txId);
            this.versionDb.EnqueueTxEntryRequest(tex.txId, getTxReq);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(2L, txEntry.CommitTime);
		}

		[TestMethod]
        public void TestSetGetCommitTs5()
        {
            //case5: test the effect of tx.maxCommitTsOfWrites
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
		public void TestSetGetCommitTs5Event()
		{
            UpdateVersionMaxCommitTsRequest req = new UpdateVersionMaxCommitTsRequest(TABLE_ID, DEFAULT_KEY, 1L, 5L);
            this.versionDb.GetVersionTable(TABLE_ID).EnqueueVersionEntryRequest(req);

			while (req.Result == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
			}

			TransactionExecution tex = new TransactionExecution(null, this.versionDb);
			while (tex.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.InitTx();
			}
			tex.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				tex.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			tex.Update(TABLE_ID, DEFAULT_KEY, "value_update_1");
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(tex.txId);
            this.versionDb.EnqueueTxEntryRequest(tex.txId, getTxReq);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(6L, txEntry.CommitTime);
		}

		[TestMethod]
        public void TestSetGetCommitTs6()
        {
            //case6: test the effect of tx.CommitLowerBound
            Transaction t1 = new Transaction(null, this.versionDb);
            this.versionDb.UpdateCommitLowerBound(t1.TxId, 5L);
            long t1CommitTs = this.versionDb.SetAndGetCommitTime(t1.TxId, 4L);
            Assert.AreEqual(5L, t1CommitTs);
        }
	}
}
