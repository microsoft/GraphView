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
            Assert.AreEqual(DEFAULT_VALUE, (string)value);
            Assert.AreEqual(1L, largestVersionKey);
        }

		[TestMethod]
		public void TestReadCase1Event()
		{
			TransactionExecution tex = new TransactionExecution(null, this.versionDb);
			tex.DEBUG_MODE = true;
			while (tex.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.InitTx();
			}
			Assert.AreNotEqual(0L, tex.txId);

			// Case1: Initial Read under event-driven senario
			tex.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				tex.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			Assert.AreEqual(DEFAULT_VALUE, payload);
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
            Assert.AreEqual(DEFAULT_VALUE, (string)value);
            Assert.AreEqual(1L, largestVersionKey);
        }

		[TestMethod]
		public void TestReadCase2Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case2: Read after local update under event-driven senario
			texUpdate.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texUpdate.Update(TABLE_ID, DEFAULT_KEY, "value_update");

			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(DEFAULT_VALUE, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
            Assert.AreEqual(DEFAULT_VALUE, (string)value);
            Assert.AreEqual(1L, largestVersionKey);
        }

		[TestMethod]
		public void TestReadCase3Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case3: Read after uploading under event-driven senario
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
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);

			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(DEFAULT_VALUE, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
            Assert.AreEqual(DEFAULT_VALUE, (string)value);
            Assert.AreEqual(1L, largestVersionKey);
        }

		[TestMethod]
		public void TestReadCase4Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case4: Read after getting commitTime under event-driven senario
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
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
			texUpdate.SetCommitTimestamp();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texUpdate.Validate), texUpdate.CurrentProc);

			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(DEFAULT_VALUE, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
            Assert.AreEqual(DEFAULT_VALUE, (string)value);
            Assert.AreEqual(1L, largestVersionKey);
        }

		[TestMethod]
		public void TestReadCase5Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case5: Read after validation under event-driven senario
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
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
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
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Validate();
			}
			Assert.AreEqual(new Procedure(texUpdate.WriteToLog), texUpdate.CurrentProc);

			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(DEFAULT_VALUE, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
            Assert.AreEqual("value_update", (string)value);
            Assert.AreEqual(2L, largestVersionKey);

        }

		[TestMethod]
		public void TestReadCase6Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case6: Read after commit under event-driven senario
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
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
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
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
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

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(texUpdate.txId);
            this.versionDb.EnqueueTxEntryRequest(texUpdate.txId, getTxReq);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(TxStatus.Committed, txEntry.Status);

			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual("value_update", (string)payloadRead);
			Assert.AreEqual(2L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
		}

		[TestMethod]
        public void TestReadCase7()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case7:Read after postprocessing
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
		public void TestReadCase7Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case7:Read after postprocessing under event-driven senario
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
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
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
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Validate();
			}
			Assert.AreEqual(new Procedure(texUpdate.WriteToLog), texUpdate.CurrentProc);
			texUpdate.WriteToLog();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.WriteToLog))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.WriteToLog();
			}
			texUpdate.PostProcessingAfterCommit();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.PostProcessingAfterCommit();
			}

			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual("value_update", (string)payloadRead);
			Assert.AreEqual(2L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
		public void TestReadCase8Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case8: Read after abort under event-driven senario
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
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
			texUpdate.SetCommitTimestamp();
			while (texUpdate.CurrentProc == new Procedure(texUpdate.SetCommitTimestamp))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.SetCommitTimestamp();
			}
			Assert.AreEqual(new Procedure(texUpdate.Validate), texUpdate.CurrentProc);
			while (texUpdate.CurrentProc != new Procedure(texUpdate.PostProcessingAfterAbort))
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.Abort();
			}
			Assert.AreEqual(new Procedure(texUpdate.PostProcessingAfterAbort), texUpdate.CurrentProc);


			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(DEFAULT_VALUE, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
		public void TestReadCase9Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

			TransactionExecution texUpdate = new TransactionExecution(null, this.versionDb);
			texUpdate.DEBUG_MODE = true;
			while (texUpdate.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texUpdate.InitTx();
			}

			// Case8: Read after abort under event-driven senario
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
				texUpdate.Upload();
			}
			Assert.AreEqual(new Procedure(texUpdate.SetCommitTimestamp), texUpdate.CurrentProc);
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
			texUpdate.PostProcessingAfterAbort();
			while (texUpdate.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texUpdate.PostProcessingAfterAbort();
			}


			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(DEFAULT_VALUE, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
		}

		[TestMethod]
        // Read [Ts, Ts', -1]
        public void TestReadCase10()
        {
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txDelete = new Transaction(null, this.versionDb);
            long largestVersionKey = 0;

            // Case10: Read after delete
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Commit();

            object value = this.ReadValue(txRead, TABLE_ID, DEFAULT_KEY, out largestVersionKey);
            Assert.AreEqual(value, null);
            Assert.AreEqual(largestVersionKey, 1L);
        }

		[TestMethod]
		public void TestReadCase10Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

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

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(texDelete.txId);
            this.versionDb.EnqueueTxEntryRequest(texDelete.txId, getTxReq);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(TxStatus.Committed, txEntry.Status);

			// Case10: Read after delete under event-driven senario.
			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(null, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
		public void TestReadCase11Event()
		{
			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			texRead.DEBUG_MODE = true;
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}

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
			texDelete.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
			texDelete.Commit();
			while (texDelete.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.CurrentProc();
			}

            GetTxEntryRequest getTxReq = new GetTxEntryRequest(texDelete.txId);
            this.versionDb.EnqueueTxEntryRequest(texDelete.txId, getTxReq);

            this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(TxStatus.Committed, txEntry.Status);

			// Case11: Read after insert under event-driven senario.
			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual("value_insert", (string)payloadRead);
			Assert.AreEqual(2L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
		public void TestReadCase12Event()
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

			// Case11: Read after local delete under event-driven senario.
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(null, (string)payloadRead);
			Assert.AreEqual(1L, texDelete.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
		public void TestReadCase13Event()
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
			texDelete.Update(TABLE_ID, DEFAULT_KEY, "value_update");

			// Case11: Read after local update under event-driven senario.
			texDelete.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texDelete.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual("value_update", (string)payloadRead);
			Assert.AreEqual(1L, texDelete.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
		}

		[TestMethod]
        // can not insert
        public void TestInsertCase1()
        {
            Transaction txInsert = new Transaction(null, this.versionDb);
            txInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY);
            txInsert.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
        }

		[TestMethod]
		public void TestInsertCase1Event()
		{
			TransactionExecution texInsert = new TransactionExecution(null, this.versionDb);
			while (texInsert.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert.InitTx();
			}
			texInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texInsert.ReadAndInitialize(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texInsert.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
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
		public void TestInsertCase2Event()
		{
			TransactionExecution texInsert = new TransactionExecution(null, this.versionDb);
			while (texInsert.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert.InitTx();
			}
			texInsert.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texInsert.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texInsert.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			texInsert.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
			texInsert.Commit();
			while (texInsert.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert.CurrentProc();
			}

			GetTxEntryRequest getTxReq = new GetTxEntryRequest(texInsert.txId);
            this.versionDb.EnqueueTxEntryRequest(texInsert.txId, getTxReq);
			this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			while (getTxReq.Result == null)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
			}
			TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
			Assert.AreEqual(TxStatus.Committed, txEntry.Status);

			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}
			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual("value_insert", (string)payloadRead);
			Assert.AreEqual(2L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
		}

		[TestMethod]
        // update -> insert
        public void TestInsertCase3()
        {
            Transaction txInsert = new Transaction(null, this.versionDb);
            txInsert.Read(TABLE_ID, DEFAULT_KEY);
            txInsert.Update(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
            txInsert.Insert(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE);
        }

		[TestMethod]
		// update -> insert
		public void TestInsertCase3Event()
		{
			TransactionExecution texInsert = new TransactionExecution(null, this.versionDb);
			while (texInsert.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texInsert.InitTx();
			}
			texInsert.Read(TABLE_ID, DEFAULT_KEY, out bool received, out object payload);
			while (!received)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				texInsert.Read(TABLE_ID, DEFAULT_KEY, out received, out payload);
			}
			texInsert.Update(TABLE_ID, DEFAULT_KEY, "value_update");
			texInsert.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
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
        public void TestUpdateCase2()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Update(TABLE_ID, DEFAULT_KEY, DEFAULT_VALUE + "_insert");
        }

		[TestMethod]
		// delete -> update
		public void TestUpdateCase2Event()
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
			tex.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			tex.Update(TABLE_ID, DEFAULT_KEY, "value_update");
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
		// delete
		public void TestDeleteCase1Event()
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
			tex.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}

			TransactionExecution texRead = new TransactionExecution(null, this.versionDb);
			while (texRead.Progress == TxProgress.Initi)
			{
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.InitTx();
			}
			texRead.Read(TABLE_ID, DEFAULT_KEY, out bool receivedRead, out object payloadRead);
			while (!receivedRead)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				texRead.Read(TABLE_ID, DEFAULT_KEY, out receivedRead, out payloadRead);
			}
			Assert.AreEqual(null, (string)payloadRead);
			Assert.AreEqual(1L, texRead.largestVersionKeyMap[TABLE_ID][DEFAULT_KEY]);
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
		// delete
		public void TestDeleteCase2Event()
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
			tex.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			tex.Insert(TABLE_ID, DEFAULT_KEY, "value_insert");
			tex.Delete(TABLE_ID, DEFAULT_KEY, out payloadDelete);
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}
		}

		[TestMethod]
        // delete -> delete
        public void TestDeleteCase3()
        {
            Transaction txDelete = new Transaction(null, this.versionDb);
            txDelete.Read(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
            txDelete.Delete(TABLE_ID, DEFAULT_KEY);
        }

		[TestMethod]
		public void TestDeleteCase3Event()
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
			tex.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			tex.Delete(TABLE_ID, DEFAULT_KEY, out payloadDelete);
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

		[TestMethod]
		// delete
		public void TestDeleteCase4Event()
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
			tex.Delete(TABLE_ID, DEFAULT_KEY, out object payloadDelete);
			tex.Commit();
			while (tex.Progress != TxProgress.Close)
			{
				this.versionDb.Visit(TABLE_ID, 0);
				this.versionDb.Visit(RedisVersionDb.TX_TABLE, 0);
				tex.CurrentProc();
			}
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
