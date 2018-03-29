using System;
using System.Collections.Generic;

namespace GraphView.Transaction
{ 
    internal partial class SingletonVersionDb : VersionDb
    {
        private static volatile SingletonVersionDb instance;
        private static readonly object initlock = new object();

        private readonly Dictionary<long, TxTableEntry> txTable;
        private readonly Dictionary<string, SingletonDictionaryVersionTable> versionTables;

        private SingletonVersionDb()
        {
            this.versionTables = new Dictionary<string, SingletonDictionaryVersionTable>();
            this.txTable = new Dictionary<long, TxTableEntry>();
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
            Random random = new Random();
            do
            {
                long txId = random.Next();
                if (!this.txTable.ContainsKey(txId))
                {
                    this.txTable[txId] = new TxTableEntry(txId);
                    return txId;
                }
            } while (true);
        }

        internal override TxTableEntry GetTxTableEntry(long txId)
        {
            return this.txTable[txId];
        }

        internal override void UpdateTxStatus(long txId, TxStatus status)
        {
            this.txTable[txId].Status = status;
        }

        internal override long GetAndSetCommitTime(long txId, long lowerBound)
        {
            throw new NotImplementedException();
        }

        internal override long UpdateCommitLowerBound(long txId, long lowerBound)
        {
            throw new NotImplementedException();
        }
    }
}
