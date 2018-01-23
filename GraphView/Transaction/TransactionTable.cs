using System.ComponentModel.Design;

namespace GraphView.Transaction
{
    using System.Collections.Generic;
    using System.Data.Linq;

    internal enum TxStatus
    {
        Active,
        Validating,
        Committed,
        Aborted,
        Terminated
    }

    internal class TxTableEntry
    {
        public TxStatus Status;
        public long BeginTimestamp;
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
    internal interface ITxTable
    {
        TxStatus GetTxStatusByTxId(long txId);
        void UpdateTxStatusByTxId(long txId, TxStatus txStatus);
        void InsertNewTx(long txId, long beginTimestamp);
        void UpdateTxEndTimestampByTxId(long txId, long endTimestamp);
    }

    /// <summary>
    /// A singleton transaction table, which stores transaction state and timestamps.
    /// This table is globally visible.
    /// </summary>
    internal class SingletonTxTable : ITxTable
    {
        private static volatile SingletonTxTable instSingletonTxTable;
        private static object initiLock = new object();
        private Dictionary<long, TxTableEntry> table;

        private SingletonTxTable()
        {
            table = new Dictionary<long, TxTableEntry>();
        }

        internal static SingletonTxTable InstSingletonTxTable
        {
            get
            {
                if (instSingletonTxTable == null)
                {
                    lock (initiLock)
                    {
                        if (instSingletonTxTable == null)
                        {
                            instSingletonTxTable = new SingletonTxTable();
                        }
                    }
                }

                return instSingletonTxTable;
            }
        }

        public TxStatus GetTxStatusByTxId(long txId)
        {
            TxTableEntry entry = null;
            if (table.TryGetValue(txId, out entry))
            {
                return entry.Status;
            }
            throw new KeyNotFoundException();
        }

        public void UpdateTxStatusByTxId(long txId, TxStatus txStatus)
        {
            if (table.ContainsKey(txId))
            {
                table[txId].Status = txStatus;
            }
            throw new KeyNotFoundException();
        }

        public void InsertNewTx(long txId, long beginTimestamp)
        {
            if (table.ContainsKey(txId))
            {
                throw new DuplicateKeyException(txId);
            }
            table.Add(txId, new TxTableEntry(TxStatus.Active, beginTimestamp, long.MaxValue));
        }

        public void UpdateTxEndTimestampByTxId(long txId, long endTimestamp)
        {
            if (table.ContainsKey(txId))
            {
                table[txId].EndTimestamp = endTimestamp;
            }
            throw new KeyNotFoundException();
        }
    }

}