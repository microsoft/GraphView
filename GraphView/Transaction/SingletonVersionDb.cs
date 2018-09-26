namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using NonBlocking;
    //using System.Collections.Concurrent;

    internal partial class SingletonVersionDb : VersionDb
    {
        private static volatile SingletonVersionDb instance;
        private static readonly object initlock = new object();

        private readonly ConcurrentDictionary<long, TxTableEntry> txTable;

        private SingletonVersionDb(int partitionCount)
            :base(partitionCount)
        {
            this.txTable = new ConcurrentDictionary<long, TxTableEntry>();
            for (int i = 0; i < partitionCount; i++)
            {
                this.dbVisitors[i] = new SingletonVersionDbVisitor(this.txTable);
            }
        }

        internal static SingletonVersionDb Instance(int partitionCount = 1)
        {
            if (SingletonVersionDb.instance == null)
            {
                lock (initlock)
                {
                    if (SingletonVersionDb.instance == null)
                    {
                        SingletonVersionDb.instance = new SingletonVersionDb(partitionCount);
                    }
                }
            }
            return SingletonVersionDb.instance;
        }
        
        internal static void DestroyInstance()
        {
            SingletonVersionDb.instance = null;
        }

        internal override void AddPartition(int partitionCount)
        {
            int prePartitionCount = this.PartitionCount;
            base.AddPartition(partitionCount);

            Array.Resize(ref this.dbVisitors, partitionCount);
            for (int pk = prePartitionCount; pk < partitionCount; pk++)
            {
                this.dbVisitors[pk] = new SingletonVersionDbVisitor(this.txTable);
            }

            foreach (VersionTable table in this.versionTables.Values)
            {
                table.AddPartition(partitionCount);
            }
        }
    }

    internal partial class SingletonVersionDb
    {
        internal override long InsertNewTx(long txId = -1)
        {
            if (txId < 0)
            {
                txId = StaticRandom.RandIdentity();

                while (!this.txTable.TryAdd(txId, new TxTableEntry(txId)))
                {
                    txId = StaticRandom.RandIdentity();
                }

                return txId;
            }
            else
            {
                return this.txTable.TryAdd(txId, new TxTableEntry(txId)) ? txId : -1;
            }
        }

        internal override bool RemoveTx(long txId)
        {
            TxTableEntry te = null;
            this.txTable.TryRemove(txId, out te);
            return te != null;
        }

        internal override bool RecycleTx(long txId)
        {
            TxTableEntry txEntry = null;
            if (this.txTable.TryGetValue(txId, out txEntry))
            {
                txEntry.Reset();
                return true;
            }
            else
            {
                return false;
            }
        }

        internal override TxTableEntry GetTxTableEntry(long txId)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(txId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            return txEntry;
        }

        internal override void UpdateTxStatus(long txId, TxStatus status)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(txId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            TxTableEntry txNewEntry = new TxTableEntry(txId, status, txEntry.CommitTime, txEntry.CommitLowerBound);
            if (!this.txTable.TryUpdate(txId, txNewEntry, txEntry))
            {
                throw new TransactionException("A tx's status has been updated by another tx concurrently.");
            }
        }

        internal override void EnqueueTxEntryRequest(long txId, TxEntryRequest txEntryRequest, int execPartition = 0)
        {
            // Interlocked.Increment(ref VersionDb.EnqueuedRequests);
            this.dbVisitors[execPartition].Invoke(txEntryRequest);
        }

        internal override long SetAndGetCommitTime(long txId, long proposedCommitTs)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(txId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            TxTableEntry newTxEntry = null;  
            while (txEntry.CommitTime < 0)
            {
                if (newTxEntry == null)
                {
                    newTxEntry = new TxTableEntry(txId);
                }

                newTxEntry.Status = txEntry.Status;
                newTxEntry.CommitTime = Math.Max(proposedCommitTs, txEntry.CommitLowerBound);
                newTxEntry.CommitLowerBound = txEntry.CommitLowerBound;

                if (this.txTable.TryUpdate(txId, newTxEntry, txEntry))
                {
                    txEntry = newTxEntry;
                }
                else
                {
                    this.txTable.TryGetValue(txId, out txEntry);
                }
            }

            return txEntry.CommitTime;
        }

        internal override long UpdateCommitLowerBound(long txId, long lowerBound)
        {
            TxTableEntry txEntry = null;
            if (!this.txTable.TryGetValue(txId, out txEntry))
            {
                throw new TransactionException("The specified tx does not exist.");
            }

            TxTableEntry newTxEntry = null;
            while (txEntry.CommitTime < 0 && txEntry.CommitLowerBound < lowerBound)
            {
                if (newTxEntry == null)
                {
                    newTxEntry = new TxTableEntry(txId);
                }

                newTxEntry.Status = txEntry.Status;
                newTxEntry.CommitTime = txEntry.CommitTime;
                newTxEntry.CommitLowerBound = lowerBound;

                if (this.txTable.TryUpdate(txId, newTxEntry, txEntry))
                {
                    txEntry = newTxEntry;
                    break;
                }
                else
                {
                    this.txTable.TryGetValue(txId, out txEntry);
                }
            }

            return txEntry.CommitTime >= 0 ? txEntry.CommitTime : -1;
        }

        internal override VersionTable CreateVersionTable(string tableId, long redisDbIndex = 0)
        {
            if (this.versionTables.ContainsKey(tableId))
            {
                return this.versionTables[tableId];
            }

            VersionTable versionTable = null; 
            lock (this.versionTables)
            {
                if (!this.versionTables.ContainsKey(tableId))
                {
                    versionTable = new SingletonDictionaryVersionTable(this, tableId, this.PartitionCount, this.txResourceManagers);
                    this.versionTables.Add(tableId, versionTable);
                }
                else
                {
                    versionTable = this.versionTables[tableId];
                }
                    
                Monitor.PulseAll(this.versionTables);
            }

            return versionTable;
        }

        internal override bool DeleteTable(string tableId)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                return true;
            }

            lock (this.versionTables)
            {
                if (this.versionTables.ContainsKey(tableId))
                {
                    this.versionTables.Remove(tableId);
                }

                Monitor.PulseAll(this.versionTables);
            }

            return true;
        }

        internal override VersionTable GetVersionTable(string tableId)
        {
            return this.versionTables[tableId];
        }

        internal override IEnumerable<string> GetAllTables()
        {
            return this.versionTables.Keys;
        }

        internal override void Clear()
        {
            this.txTable.Clear();

			foreach (string tableId in this.versionTables.Keys)
			{
				VersionTable versionTable = this.versionTables[tableId];
				versionTable.Clear();
			}

            this.versionTables.Clear();
		}

        internal override void MockLoadData(int recordCount)
        {
            foreach (VersionTable table in this.versionTables.Values)
            {
                table.MockLoadData(recordCount);
            }
        }
    }
}
