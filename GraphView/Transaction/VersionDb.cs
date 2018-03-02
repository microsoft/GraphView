
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using RecordRuntime;

    /// <summary>
    /// A version Db for concurrency control.
    /// </summary>
    public abstract partial class VersionDb
    { 
        /// <summary>
        /// Get versionTable instance by tableId
        /// </summary>
        /// <param name="tableId"></param>
        /// <returns>VersionTable object or null if the table is not found</returns>
        internal virtual VersionTable GetVersionTable(string tableId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Read the legal version from one of the version table, if not found, return null. 
        /// </summary>
        internal virtual VersionEntry ReadVersion(
            string tableId,
            object recordKey,
            long readTimestamp)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return null;
            }
            return versionTable.ReadVersion(recordKey, readTimestamp);
        }

        /// <summary>
        /// Insert a new version
        /// Insert the version to one of the version table, using the version table's InsertVersion() method.
        /// Should be overrided by the inherited class for synchronize safety of datastruct in versionDb
        /// </summary>
        internal virtual bool InsertVersion(
            string tableId,
            object recordKey,
            object record,
            long txId,
            long readTimestamp)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Delete an existing version.
        /// First check the existence of the corresponding version table.
        /// If exists, delete the version, using the version table's DeleteVersion() method.
        /// </summary>
        internal virtual bool DeleteVersion(
            string tableId,
            object recordKey,
            long txId,
            long readTimestamp,
            out VersionEntry deletedVersion)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                deletedVersion = null;
                return true;
            }
            return versionTable.DeleteVersion(recordKey, txId, readTimestamp, out deletedVersion);
        }

        /// <summary>
        /// Update an existing version.
        /// First check the existence of the corresponding version table.
        /// If exists, update the version, using the version table's UpdateVersion() method.
        /// </summary>
        internal virtual bool UpdateVersion(
            string tableId,
            object recordKey,
            object record,
            long txId,
            long readTimestamp,
            out VersionEntry oldVersion,
            out VersionEntry newVersion)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                //the corresponding version table does not exist.
                oldVersion = null;
                newVersion = null;
                return false;
            }
            return versionTable.UpdateVersion(recordKey, record, txId, readTimestamp, 
                out oldVersion, out newVersion);
        }

        /// <summary>
        /// Check visibility of the version read before, used in validation phase.
        /// First check the existence of the corresponding version table.
        /// If exists, check the version, using the version table's CheckVersionVisibility() method.
        /// </summary>
        internal virtual bool CheckReadVisibility(
            string tableId,
            object recordKey,
            long readVersionBeginTimestamp,
            long readTimestamp,
            long txId,
            TransactionTable txTable)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return false;
            }
            return versionTable.CheckReadVisibility(recordKey, readVersionBeginTimestamp,
                readTimestamp, txId, txTable);
        }

        /// <summary>
        /// Check for Phantom.
        /// First check the existence of the corresponding version table.
        /// If exists, check the version, using the version table's CheckPhantom() method.
        /// </summary>
        internal virtual bool CheckPhantom(
            string tableId,
            object recordKey,
            long oldScanTime,
            long newScanTime,
            long txId,
            TransactionTable txTable)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return true;
            }
            return versionTable.CheckPhantom(recordKey, oldScanTime, newScanTime, txId, txTable);
        }

        /// <summary>
        /// First check the existence of the corresponding version table.
        /// If exists, udpate the version's timestamp, using the version table's UpdateCommittedVersionTimestamp() method.
        /// </summary>
        internal virtual void UpdateCommittedVersionTimestamp(
            string tableId,
            object recordKey,
            long txId,
            long endTimestamp)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return;
            }
            versionTable.UpdateCommittedVersionTimestamp(recordKey, txId, endTimestamp);
        }

        /// <summary>
        /// First check the existence of the corresponding version table.
        /// If exists, udpate the version's timestamp, using the version table's UpdateAbortedVersionTimestamp() method.
        /// </summary>
        internal virtual void UpdateAbortedVersionTimestamp(
            string tableId,
            object recordKey,
            long txId)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return;
            }
            versionTable.UpdateAbortedVersionTimestamp(recordKey, txId);
        }
    }

    public abstract partial class VersionDb : IVersionedDataStore
    {
        public bool DeleteJson(string tableId, object key, Transaction tx)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                throw new ArgumentException($"Invalid tableId reference '{tableId}'");
            }
            return versionTable.DeleteJson(key, tx);
        }

        public JObject GetJson(string tableId, object key, Transaction tx)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                throw new ArgumentException($"Invalid tableId reference '{tableId}'");
            }
            return versionTable.GetJson(key, tx);
        }

        public IList<JObject> GetRangeJsons(string tableId, object lowerKey, object upperKey, Transaction tx)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                throw new ArgumentException($"Invalid tableId reference '{tableId}'");
            }
            return versionTable.GetRangeJsons(lowerKey, upperKey, tx);
        }

        public IList<object> GetRangeRecordKeyList(string tableId, object lowerValue, object upperValue, Transaction tx)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                throw new ArgumentException($"Invalid tableId reference '{tableId}'");
            }
            return versionTable.GetRangeRecordKeyList(lowerValue, upperValue, tx);
        }

        public IList<object> GetRecordKeyList(string tableId, object value, Transaction tx)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                throw new ArgumentException($"Invalid tableId reference '{tableId}'");
            }
            return versionTable.GetRecordKeyList(value, tx);
        }

        public bool InsertJson(string tableId, object key, JObject record, Transaction tx)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                throw new ArgumentException($"Invalid tableId reference '{tableId}'");
            }
            return versionTable.InsertJson(key, record, tx);
        }

        public bool UpdateJson(string tableId, object key, JObject record, Transaction tx)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                throw new ArgumentException($"Invalid tableId reference '{tableId}'");
            }
            return versionTable.UpdateJson(key, record, tx);
        }
    }
}
