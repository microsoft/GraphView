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
		// case1: upload new inserted entry successfully under event-driven senario.
		public void TestUploadNewCase1Event()
		{
			TransactionExecution texDelete = new TransactionExecution(null, this.versionDb);
			while (texDelete.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.InitTx();
			}
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texDelete.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texDelete.Commit();
			while (texDelete.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.CurrentProc();
			}

			TransactionExecution texInsert = new TransactionExecution(null, this.versionDb);
			texInsert.DEBUG_MODE = true;
			while (texInsert.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert.InitTx();
			}
			texInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texInsert.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
			texInsert.Upload();
			while (texInsert.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texInsert.Upload();
			}
			Assert.AreEqual(new Procedure(texInsert.SetCommitTimestamp), texInsert.CurrentProc);
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
		// case2: upload new inserted entry failed under event-driven senario.
		public void TestUploadNewCase2Event()
		{
			TransactionExecution texDelete = new TransactionExecution(null, this.versionDb);
			while (texDelete.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.InitTx();
			}
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texDelete.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texDelete.Commit();
			while (texDelete.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.CurrentProc();
			}

			TransactionExecution texInsert = new TransactionExecution(null, this.versionDb);
			texInsert.DEBUG_MODE = true;
			while (texInsert.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert.InitTx();
			}
			texInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texInsert.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
			texInsert.Upload();
			while (texInsert.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texInsert.Upload();
			}
			Assert.AreEqual(new Procedure(texInsert.SetCommitTimestamp), texInsert.CurrentProc);


			TransactionExecution texInsert1 = new TransactionExecution(null, this.versionDb);
			texInsert1.DEBUG_MODE = true;
			while (texInsert1.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert1.InitTx();
			}
			texInsert1.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out bool receivedInsert1, out object payloadInsert1);
			while (!receivedInsert1)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert1.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out receivedInsert1, out payloadInsert1);
			}
			texInsert1.Insert(TABLE_ID, DEFAULT_KEY, "value_insert1");
			texInsert1.Upload();
			while (texInsert1.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert1.Upload();
			}
			Assert.AreEqual(new Procedure(texInsert1.Abort), texInsert1.CurrentProc);
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
		public void TestReplaceOldCase1Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
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
		public void TestReplaceOldCase2Event()
		{
			TransactionExecution texDelete = new TransactionExecution(null, this.versionDb);
			texDelete.DEBUG_MODE = true;
			while (texDelete.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.InitTx();
			}
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texDelete.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texDelete.Upload();
			while (texDelete.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Upload();
			}
			Assert.AreEqual(new Procedure(texDelete.SetCommitTimestamp), texDelete.CurrentProc);

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.Abort), texUpdate.CurrentProc);
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
		public void TestReplaceOldCase3Event()
		{
			TransactionExecution texDelete = new TransactionExecution(null, this.versionDb);
			texDelete.DEBUG_MODE = true;
			while (texDelete.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.InitTx();
			}
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texDelete.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texDelete.Upload();
			while (texDelete.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Upload();
			}
			Assert.AreEqual(new Procedure(texDelete.SetCommitTimestamp), texDelete.CurrentProc);

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");

			texDelete.SetCommitTimestamp();
			while (texDelete.CurrentProc == new Procedure(texDelete.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texDelete.Validate), texDelete.CurrentProc);
			texDelete.Validate();
			while (texDelete.CurrentProc == new Procedure(texDelete.Validate))
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Validate();
			}
			Assert.AreEqual(new Procedure(texDelete.WriteToLog), texDelete.CurrentProc);
			texDelete.WriteToLog();
			while (texDelete.CurrentProc == new Procedure(texDelete.WriteToLog))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.WriteToLog();
			}
			Assert.AreEqual(new Procedure(texDelete.PostProcessingAfterCommit), texDelete.CurrentProc);

			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.Abort), texUpdate.CurrentProc);
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
		public void TestReplaceOldCase4Event()
		{
			TransactionExecution texDelete = new TransactionExecution(null, this.versionDb);
			texDelete.DEBUG_MODE = true;
			while (texDelete.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.InitTx();
			}
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texDelete.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texDelete.Upload();
			while (texDelete.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texDelete.Upload();
			}
			Assert.AreEqual(new Procedure(texDelete.SetCommitTimestamp), texDelete.CurrentProc);
			texDelete.SetCommitTimestamp();
			while (texDelete.CurrentProc == new Procedure(texDelete.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texDelete.Validate), texDelete.CurrentProc);
			texDelete.Abort();
			while (texDelete.CurrentProc != new Procedure(texDelete.PostProcessingAfterAbort))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.Abort();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
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
		public void TestRepalceOldCase5Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texUpdate.Validate), texUpdate.CurrentProc);
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
			Assert.AreEqual(new Procedure(texUpdate.PostProcessingAfterCommit), texUpdate.CurrentProc);

			TransactionExecution texUpdate1 = new TransactionExecution(null, this.versionDb);
			texUpdate1.DEBUG_MODE = true;
			while (texUpdate1.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.InitTx();
			}
			texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead1, out object payloadRead1);
			while (!receivedRead1)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out receivedRead1, out payloadRead1);
			}
			texUpdate1.Update(TABLE_ID, DEFAULT_KEY, "value_update_1");
			texUpdate1.Upload();
			while (texUpdate1.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate1.SetCommitTimestamp), texUpdate1.CurrentProc);
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
		public void TestReplaceOldCase6Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texUpdate.Validate), texUpdate.CurrentProc);
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
			Assert.AreEqual(new Procedure(texUpdate.PostProcessingAfterCommit), texUpdate.CurrentProc);

			TransactionExecution texUpdate1 = new TransactionExecution(null, this.versionDb);
			texUpdate1.DEBUG_MODE = true;
			while (texUpdate1.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.InitTx();
			}
			texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead1, out object payloadRead1);
			while (!receivedRead1)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out receivedRead1, out payloadRead1);
			}

			texUpdate.PostProcessingAfterCommit();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.PostProcessingAfterCommit();
			}

			texUpdate1.Update(TABLE_ID, DEFAULT_KEY, "value_update_1");
			texUpdate1.Upload();
			while (texUpdate1.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate1.SetCommitTimestamp), texUpdate1.CurrentProc);
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
		public void TestReplaceOldCase7Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);

			TransactionExecution texUpdate1 = new TransactionExecution(null, this.versionDb);
			texUpdate1.DEBUG_MODE = true;
			while (texUpdate1.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.InitTx();
			}
			texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead1, out object payloadRead1);
			while (!receivedRead1)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out receivedRead1, out payloadRead1);
			}

			texUpdate.SetCommitTimestamp();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texUpdate.Validate), texUpdate.CurrentProc);
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
			Assert.AreEqual(new Procedure(texUpdate.PostProcessingAfterCommit), texUpdate.CurrentProc);

			texUpdate1.Update(TABLE_ID, DEFAULT_KEY, "value_update_1");
			texUpdate1.Upload();
			while (texUpdate1.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate1.Abort), texUpdate1.CurrentProc);
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
		public void TestReplaceOldCase8Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);

			TransactionExecution texUpdate1 = new TransactionExecution(null, this.versionDb);
			texUpdate1.DEBUG_MODE = true;
			while (texUpdate1.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.InitTx();
			}
			texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead1, out object payloadRead1);
			while (!receivedRead1)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out receivedRead1, out payloadRead1);
			}

			texUpdate.SetCommitTimestamp();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texUpdate.Validate), texUpdate.CurrentProc);
			texUpdate.Abort();
			while (texUpdate.CurrentProc != new Procedure(texUpdate.PostProcessingAfterAbort))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Abort();
			}

			texUpdate1.Update(TABLE_ID, DEFAULT_KEY, "value_update_1");
			texUpdate1.Upload();
			while (texUpdate1.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate1.SetCommitTimestamp), texUpdate1.CurrentProc);
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

		[TestMethod]
		public void TestReplaceOldCase9Event()
		{
			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			texUpdate.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texUpdate.Upload();
			while (texUpdate.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);

			TransactionExecution texUpdate1 = new TransactionExecution(null, this.versionDb);
			texUpdate1.DEBUG_MODE = true;
			while (texUpdate1.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.InitTx();
			}
			texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead1, out object payloadRead1);
			while (!receivedRead1)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Read(TABLE_ID, DEFAULT_KEY, out receivedRead1, out payloadRead1);
			}

			texUpdate.SetCommitTimestamp();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texUpdate.Validate), texUpdate.CurrentProc);
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
			Assert.AreEqual(new Procedure(texUpdate.PostProcessingAfterCommit), texUpdate.CurrentProc);
			texUpdate.PostProcessingAfterCommit();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.PostProcessingAfterCommit();
			}

			texUpdate1.Update(TABLE_ID, DEFAULT_KEY, "value_update_1");
			texUpdate1.Upload();
			while (texUpdate1.CurrentProc == null)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate1.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate1.Abort), texUpdate1.CurrentProc);
		}
	}
}
