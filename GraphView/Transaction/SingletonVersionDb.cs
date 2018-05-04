
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using NonBlocking;

    internal partial class SingletonVersionDb : VersionDb
    {
        private static volatile SingletonVersionDb instance;
        private static readonly object initlock = new object();

        private readonly NonBlocking.ConcurrentDictionary<long, TxTableEntry> txTable;
        private readonly Dictionary<string, SingletonDictionaryVersionTable> versionTables;

        private SingletonVersionDb()
        {
            this.versionTables = new Dictionary<string, SingletonDictionaryVersionTable>();
            this.txTable = new ConcurrentDictionary<long, TxTableEntry>();
        }

        internal static SingletonVersionDb Instance
        {
            get
            {
                if (SingletonVersionDb.instance == null)
                {
                    lock (initlock)
                    {
                        if (SingletonVersionDb.instance == null)
                        {
                            SingletonVersionDb.instance = new SingletonVersionDb();
                        }
                    }
                }
                return SingletonVersionDb.instance;
            }
        }


    }

    internal partial class SingletonVersionDb
    {
        //Todo: solve the problem of inserting at the same time
        internal override long InsertNewTx()
        {
            long txId = StaticRandom.Rand();

            while (!this.txTable.TryAdd(txId, new TxTableEntry(txId)))
            {
                txId = StaticRandom.Rand();
            }

            return txId;
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

            txEntry.Status = status;
        }

        internal override long SetAndGetCommitTime(long txId, long lowerBound)
        {
            throw new NotImplementedException();
        }

        internal override long UpdateCommitLowerBound(long txId, long lowerBound)
        {
            throw new NotImplementedException();
        }

        internal override VersionTable CreateVersionTable(string tableId, long redisDbIndex = 0)
        {
            return base.CreateVersionTable(tableId, redisDbIndex);
        }
    }
}
