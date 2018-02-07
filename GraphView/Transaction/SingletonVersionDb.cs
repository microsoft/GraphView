

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

    internal class IndexSpecification
    {
        // A list of properties to be indexed
        IList<string> properties;
    }

    internal class SingletonVersionDb : VersionDb, IVersionedDataStore
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

        public JObject GetJson(string tableId, object key, Transaction tx)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                throw new ArgumentException($"Invalid table reference '{tableId}'");
            }

            return this.versionTables[tableId].GetJson(key, tx);
        }

        public IList<JObject> GetRangeJsons(string tableId, object lowerKey, object upperKey, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRangeRecordKeyList(string tableId, object lowerValue, object upperValue, Transaction tx)
        {
            throw new NotImplementedException();
        }

        public IList<object> GetRecordKeyList(string tableId, object value, Transaction tx)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Read the legal version from one of the version table, if not find, return null. 
        /// </summary>
        internal override VersionEntry ReadVersion(string tableId, object recordKey, long readTimestamp)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                return null;
            }

            return this.versionTables[tableId].ReadVersion(recordKey, readTimestamp);
        }

        /// <summary>
        /// Insert a new version.
        /// Insert the version to one of the version table, using the version table's InsertVersion() method.
        /// </summary>
        internal override bool InsertVersion(string tableId, object recordKey, JObject record, long txId, long readTimestamp)
        {
            //If the corresponding version table does not exist, create a new one.
            //Use lock to ensure thread synchronization.
            if (!this.versionTables.ContainsKey(tableId))
            {
                lock (this.tableLock)
                {
                    if (!this.versionTables.ContainsKey(tableId))
                    {
                        this.versionTables[tableId] = new SingletonVersionDictionary();
                    }
                }
            }
            //insert
            return this.versionTables[tableId].InsertVersion(recordKey, record, txId, readTimestamp);
        }

        /// <summary>
        /// Update an existing version.
        /// First check the existence of the corresponding version table.
        /// If exists, update the version, using the version table's UpdateVersion() method.
        /// </summary>
        internal override bool UpdateVersion(string tableId, object recordKey, JObject record, long txId, long readTimestamp,
            out VersionEntry oldVersion, out VersionEntry newVersion)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                //the corresponding version table does not exist.
                oldVersion = null;
                newVersion = null;
                return false;
            }

            return this.versionTables[tableId]
                .UpdateVersion(recordKey, record, txId, readTimestamp, out oldVersion, out newVersion);
        }

        /// <summary>
        /// Delete an existing version.
        /// First check the existence of the corresponding version table.
        /// If exists, delete the version, using the version table's DeleteVersion() method.
        /// </summary>
        internal override bool DeleteVersion(string tableId, object recordKey, long txId, long readTimestamp, 
            out VersionEntry deletedVersion)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                //the corresponding version table does not exist.
                deletedVersion = null;
                return true;
            }

            return this.versionTables[tableId].DeleteVersion(recordKey, txId, readTimestamp, out deletedVersion);
        }

        /// <summary>
        /// Check visibility of the version read before, used in validation phase.
        /// First check the existence of the corresponding version table.
        /// If exists, check the version, using the version table's CheckVersionVisibility() method.
        /// </summary>
        internal override bool CheckReadVisibility(string tableId, object recordKey, long readVersionBeginTimestamp, 
            long readTimestamp, long txId)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                //the corresponding version table does not exist.
                return false;
            }

            return this.versionTables[tableId]
                .CheckReadVisibility(recordKey, readVersionBeginTimestamp, readTimestamp, txId);
        }

        /// <summary>
        /// Check for Phantom.
        /// First check the existence of the corresponding version table.
        /// If exists, check the version, using the version table's CheckPhantom() method.
        /// </summary>
        internal override bool CheckPhantom(string tableId, object recordKey, long oldScanTime, long newScanTime)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                //the corresponding version table does not exist.
                return true;
            }

            return this.versionTables[tableId].CheckPhantom(recordKey, oldScanTime, newScanTime);
        }

        /// <summary>
        /// First check the existence of the corresponding version table.
        /// If exists, udpate the version's timestamp, using the version table's UpdateCommittedVersionTimestamp() method.
        /// </summary>
        internal override void UpdateCommittedVersionTimestamp(string tableId, object recordKey, long txId, long endTimestamp)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                //the corresponding version table does not exist.
                return;
            }

            this.versionTables[tableId].UpdateCommittedVersionTimestamp(recordKey, txId, endTimestamp);
        }

        /// <summary>
        /// First check the existence of the corresponding version table.
        /// If exists, udpate the version's timestamp, using the version table's UpdateAbortedVersionTimestamp() method.
        /// </summary>
        internal override void UpdateAbortedVersionTimestamp(string tableId, object recordKey, long txId)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                //the corresponding version table does not exist.
                return;
            }

            this.versionTables[tableId].UpdateAbortedVersionTimestamp(recordKey, txId);
        }
    }

}
