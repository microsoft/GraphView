using System.ComponentModel.Design;


namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Data.Linq;

    public enum TxStatus
    {
        Active,
        Committed,
        Aborted
    }

    internal class TxTableEntry
    {
        public TxStatus Status;
        public readonly long BeginTimestamp;
        public long EndTimestamp;

        public TxTableEntry(TxStatus txStatus, long beginTimestamp, long endTimestamp)
        {
            this.Status = txStatus;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
        }
    }

    /// <summary>
    /// An interface for the transaction table.
    /// </summary>
    public abstract class TransactionTable
    {
        internal virtual TxStatus GetTxStatusByTxId(long txId)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateTxStatusByTxId(long txId, TxStatus txStatus)
        {
            throw new NotImplementedException();
        }

        internal virtual void InsertNewTx(long txId, long beginTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateTxEndTimestampByTxId(long txId, long endTimestamp)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// A singleton transaction table, which stores transaction state and timestamps.
    /// This table is globally visible.
    /// </summary>
    internal class SingletonTxTable : TransactionTable
    {
        private static volatile SingletonTxTable instSingletonTxTable;
        private static object initiLock = new object();
        private Dictionary<long, TxTableEntry> table;
        
        private SingletonTxTable()
        {
            this.table = new Dictionary<long, TxTableEntry>();
        }

        internal static SingletonTxTable InstSingletonTxTable
        {
            get
            {
                if (SingletonTxTable.instSingletonTxTable == null)
                {
                    lock (SingletonTxTable.initiLock)
                    {
                        if (SingletonTxTable.instSingletonTxTable == null)
                        {
                            SingletonTxTable.instSingletonTxTable = new SingletonTxTable();
                        }
                    }
                }

                return SingletonTxTable.instSingletonTxTable;
            }
        }

        internal override TxStatus GetTxStatusByTxId(long txId)
        {
            TxTableEntry entry = null;
            if (this.table.TryGetValue(txId, out entry))
            {
                return entry.Status;
            }
            throw new KeyNotFoundException();
        }

        internal override void UpdateTxStatusByTxId(long txId, TxStatus txStatus)
        {
            if (this.table.ContainsKey(txId))
            {
                table[txId].Status = txStatus;
            }
            throw new KeyNotFoundException();
        }

        internal override void InsertNewTx(long txId, long beginTimestamp)
        {
            if (this.table.ContainsKey(txId))
            {
                throw new DuplicateKeyException(txId);
            }
            this.table.Add(txId, new TxTableEntry(TxStatus.Active, beginTimestamp, long.MinValue));
        }

        internal override void UpdateTxEndTimestampByTxId(long txId, long endTimestamp)
        {
            if (this.table.ContainsKey(txId))
            {
                this.table[txId].EndTimestamp = endTimestamp;
            }
            throw new KeyNotFoundException();
        }
    }

}
