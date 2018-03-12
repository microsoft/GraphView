

using System.Runtime.InteropServices.WindowsRuntime;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.RecordRuntime;
    using Newtonsoft.Json.Linq;

    internal partial class SingletonVersionDb : VersionDb, IVersionedDataStore
    {
        private static volatile SingletonVersionDb instance;
        private static readonly object initlock = new object();
        private readonly Dictionary<string, SingletonVersionTable> versionTables;
        private readonly object tableLock;

        // A map from a table to its index tables
        private Dictionary<string, IList<Tuple<string, IndexSpecification>>> indexMap;

        private SingletonVersionDb()
        {
            this.versionTables = new Dictionary<string, SingletonVersionTable>();
            this.tableLock = new object();
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
        internal override VersionTable GetVersionTable(string tableId)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                return null;
            }

            return this.versionTables[tableId];
        }

        internal override bool InsertVersion(string tableId,
            object recordKey,
            object record,
            long txId,
            long readTimestamp)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            //If the corresponding version table does not exist, create a new one.
            //Use lock to ensure thread synchronization.
            if (versionTable == null)
            {
                lock (this.tableLock)
                {
                    if (this.GetVersionTable(tableId) == null)
                    {
                        this.versionTables[tableId] = new SingletonVersionDictionary(tableId);
                    }
                }
            }
            return this.GetVersionTable(tableId).InsertVersion(recordKey, record, txId, readTimestamp);
        }
    }

    internal partial class SingletonVersionDb : IDataStore
    {
        public bool CreateTable(string tableId)
        {
            throw new NotImplementedException();
        }

        public bool DeleteTable(string tableId)
        {
            throw new NotImplementedException();
        }

        public IList<Tuple<string, IndexSpecification>> GetIndexTables(string tableId)
        {
            if (!indexMap.ContainsKey(tableId))
            {
                throw new ArgumentException($"Invalid table reference '{tableId}'");
            }
            return this.indexMap[tableId];
        }

        public IList<string> GetTables()
        {
            return this.versionTables.Keys.ToList();
        }
    }
}
