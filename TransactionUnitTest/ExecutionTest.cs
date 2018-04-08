namespace GraphViewUnitTest.Transaction
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using GraphView.Transaction;
    using ServiceStack.Redis;
    using System.Collections.Generic;
    using TransactionUnitTest;

    [TestClass]
    public class ExecutionTest : AbstractTransactionTest
    {
        /// <summary>
        /// Define our own setup methods
        /// </summary>
        public void SetUp()
        {
            using (RedisClient redisClient = (RedisClient)this.clientManager.GetClient())
            {
                // 1. flush the test db
                redisClient.ChangeDb(ExecutionTest.TEST_REDIS_DB);
                redisClient.FlushDb();

                // 2. create version table
                this.versionDb.CreateVersionTable(ExecutionTest.TABLE_ID, ExecutionTest.TEST_REDIS_DB);

                // 3. load data
                Transaction tx = new Transaction(null, this.versionDb);
                tx.ReadAndInitialize(ExecutionTest.TABLE_ID, "key");
                tx.Insert(ExecutionTest.TABLE_ID, "key", "value");
                tx.Commit();
            }
        }

        /// <summary>
        /// Test read the most recent records
        /// </summary>
        [TestMethod]
        public void TestRead()
        {
            this.SetUp();

            // command variables
            object value = null;
            long largestVersionKey = 0;

            // two transactions
            Transaction txRead = new Transaction(null, this.versionDb);
            Transaction txUpdate = new Transaction(null, this.versionDb);

            // Case1: Initial Read
            value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string) value, "value");
            Assert.AreEqual(largestVersionKey, 1L);

            // Case2: Read after local update
            txUpdate.Read(ExecutionTest.TABLE_ID, "key");
            txUpdate.Update(ExecutionTest.TABLE_ID, "key", "value_update");
            value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);

            // Case3: Read after uploading
            txUpdate.UploadLocalWriteRecords();
            value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);

            // Case4: Read after getting commitTime
            txUpdate.GetCommitTimestamp();
            value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);

            // Case5: Read after validation
            txUpdate.Validate();
            value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value");
            Assert.AreEqual(largestVersionKey, 1L);

            // Case6: Read after commit
            this.versionDb.UpdateTxStatus(txUpdate.TxId, TxStatus.Committed);
            value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value_update");
            Assert.AreEqual(largestVersionKey, 2L);

            // Case7: Read after postprocessing
            txUpdate.PostProcessingAfterCommit();
            value = this.ReadValue(txRead, ExecutionTest.TABLE_ID, "key", out largestVersionKey);
            Assert.AreEqual((string)value, "value_update");
            Assert.AreEqual(largestVersionKey, 2L);

            // Cases for aborted

            // Cases for deletion

            // Cases for insertion
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
