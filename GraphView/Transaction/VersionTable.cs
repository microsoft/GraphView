
using ServiceStack.Text;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using GraphView.RecordRuntime;

    /// <summary>
    /// A version table for concurrency control.
    /// </summary>
    public abstract partial class VersionTable
    {
        public readonly string tableId;

        public VersionTable(string tableId)
        {
            this.tableId = tableId;
        }

        /// <summary>
        /// Given a reocrd key, returns the record's version list. 
        /// It is desirable for the version table to only return a small fragment of the list  
        /// that contains the version entry visible to the caller transaction. 
        /// </summary>
        /// <param name="recordKey">The record key</param>
        /// <returns>A list of versions of the input record</returns>
        internal virtual IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Given a batch of record keys, retrieves their verion lists in a batch
        /// </summary>
        /// <param name="recordKeys"></param>
        /// <returns></returns>
        internal virtual IDictionary<object, IEnumerable<VersionEntry>> GetVersionList(IEnumerable<object> recordKeys)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Given a record key and the read timestamp, returns a version entry visible w.r.t. the timestamp.
        /// This method should be overriden whenever the underlying key-value store provides a more efficient 
        /// way of locating the visible version entry than scanning the full version list.
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        internal virtual VersionEntry GetVersionEntryByTimestamp(object recordKey, long timestamp, TransactionTable txTable, ref DependencyTable depTable)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);

            if (versionList == null)
            {
                return null;
            }

            foreach (VersionEntry version in versionList)
            {
                if (this.CheckVersionVisibility(version, timestamp, txTable, ref depTable))
                {
                    return version;
                }
            }

            return null;
        }

        /// <summary>
        /// Given a record key and a verion key, returns the version entry by the composite key. 
        /// This method should be overriden whenever the underlying key-value store provides 
        /// a more efficient way of locating the verion entry, e.g., through key lookups, than
        /// scanning the full version list.
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <returns></returns>
        internal virtual VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);

            if (versionList == null)
            {
                return null;
            }

            foreach (VersionEntry version in versionList)
            {
                if (version.RecordKey == recordKey && version.VersionKey == versionKey)
                {
                    return version;
                }
            }

            return null;
        }

        /// <summary>
        /// Given a batch of verion keys, retrieves a collection of verion entries in a batch
        /// </summary>
        /// <param name="batch"></param>
        /// <returns></returns>
        internal virtual IDictionary<Tuple<object, long>, VersionEntry> GetVersionEntryByKey(
            IEnumerable<Tuple<object, long>> batch)
        {
            throw new NotImplementedException();
        }

        internal virtual bool InsertAndUploadVersion(object recordKey, VersionEntry version)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateAndUploadVersion(object recordKey, long versionKey, VersionEntry oldVersion, VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }
    }

    public abstract partial class VersionTable
    {
        /// <summary>
        /// Given a record's version and a Tx's timestamp (or a TxId), check this version's visibility.
        /// </summary>
        /// <param name="version">A version entry of a record</param>
        /// <param name="readTimestamp">Tx's timestamp</param>
        /// <returns>True, if the input version is visible to the transaction. False, otherwise.</returns>
        internal bool CheckVersionVisibility(VersionEntry version, long readTimestamp, TransactionTable txTable, ref DependencyTable depTable)
        {
            //case 1: both begin and end fields are timestamp
            //just check whether readTimestamp is in the interval of the version's beginTimestamp and endTimestamp 
            if (!version.IsBeginTxId && !version.IsEndTxId)
            {
                return readTimestamp > version.BeginTimestamp && readTimestamp < version.EndTimestamp;
            }
            //case 2: begin field is a TxId, end field is a timestamp
            else if (version.IsBeginTxId && !version.IsEndTxId)
            {
                TxStatus status = txTable.GetTxStatusByTxId(version.BeginTimestamp);
                if (status == TxStatus.Active)
                {
                    //visible only if this version is created by the same transaction
                    return version.BeginTimestamp == readTimestamp && version.EndTimestamp == long.MaxValue;
                }
                else if (status == TxStatus.Preparing)
                {
                    //may speculatively read
                    long txEndTimestamp = txTable.GetTxEndTimestampByTxId(version.BeginTimestamp);
                    if (readTimestamp > txEndTimestamp && readTimestamp < version.EndTimestamp)
                    {
                        depTable.AddCommitDependency(readTimestamp, txEndTimestamp, true);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (status == TxStatus.Committed)
                {
                    return readTimestamp > txTable.GetTxEndTimestampByTxId(version.BeginTimestamp) &&
                           readTimestamp < version.EndTimestamp;
                }
                else
                {
                    return false;
                }
            }
            //case 3: begin field is a TxId, end field is a TxId
            else if (version.IsBeginTxId && version.IsEndTxId)
            {
                TxStatus beginTxStatus = txTable.GetTxStatusByTxId(version.BeginTimestamp);
                if (beginTxStatus == TxStatus.Active || beginTxStatus == TxStatus.Aborted)
                {
                    return false;
                }
                else if (beginTxStatus == TxStatus.Committed)
                {
                    long beginTxEndTimestamp = txTable.GetTxEndTimestampByTxId(version.BeginTimestamp);
                    if (readTimestamp < beginTxEndTimestamp)
                    {
                        return false;
                    }
                    else
                    {
                        TxStatus endTxStatus = txTable.GetTxStatusByTxId(version.EndTimestamp);
                        if (endTxStatus == TxStatus.Active)
                        {
                            //other transaction can see this old version
                            return readTimestamp != version.EndTimestamp;
                        }
                        else if (endTxStatus == TxStatus.Preparing)
                        {
                            long endTxEndTimestamp = txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                            if (readTimestamp < endTxEndTimestamp)
                            {
                                return true;
                            }
                            else if (readTimestamp > endTxEndTimestamp)
                            {
                                depTable.AddCommitDependency(readTimestamp, version.EndTimestamp, true);
                                return false;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        else if (endTxStatus == TxStatus.Committed)
                        {
                            return readTimestamp < txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                        }
                        else
                        {
                            return true;
                        }
                    }
                }
                else
                {
                    long beginTxEndTimestamp = txTable.GetTxEndTimestampByTxId(version.BeginTimestamp);
                    if (readTimestamp < beginTxEndTimestamp)
                    {
                        return false;
                    }
                    else
                    {
                        TxStatus endTxStatus = txTable.GetTxStatusByTxId(version.EndTimestamp);
                        if (endTxStatus == TxStatus.Active)
                        {
                            //other transaction can see this old version
                            //return readTimestamp != version.EndTimestamp;
                            if (readTimestamp == version.EndTimestamp)
                            {
                                return false;
                            }
                            else
                            {
                                depTable.AddCommitDependency(readTimestamp, version.BeginTimestamp, true);
                                return true;
                            }
                        }
                        else if (endTxStatus == TxStatus.Preparing)
                        {
                            long endTxEndTimestamp = txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                            if (readTimestamp < endTxEndTimestamp)
                            {
                                depTable.AddCommitDependency(readTimestamp, version.BeginTimestamp, true);
                                return true;
                            }
                            else if (readTimestamp > endTxEndTimestamp)
                            {
                                //depTable.AddCommitDependency(readTimestamp, version.BeginTimestamp, true);
                                depTable.AddCommitDependency(readTimestamp, version.EndTimestamp, true);
                                return false;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        else if (endTxStatus == TxStatus.Committed)
                        {
                            if (readTimestamp < txTable.GetTxEndTimestampByTxId(version.EndTimestamp))
                            {
                                depTable.AddCommitDependency(readTimestamp, version.BeginTimestamp, true);
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        else
                        {
                            depTable.AddCommitDependency(readTimestamp, version.BeginTimestamp, true);
                            return true;
                        }
                    }
                }
            }
            //case 4: begin field is a timestamp, end field is a TxId
            else
            {
                TxStatus status = txTable.GetTxStatusByTxId(version.EndTimestamp);
                if (status == TxStatus.Active)
                {
                    //other transaction can see this old version
                    return readTimestamp > version.BeginTimestamp && readTimestamp != version.EndTimestamp;
                }
                else if (status == TxStatus.Preparing)
                {
                    long txEndTimestamp = txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                    if (readTimestamp > version.BeginTimestamp && readTimestamp < txEndTimestamp)
                    {
                        return true;
                    }
                    else if (readTimestamp > version.BeginTimestamp && readTimestamp > txEndTimestamp)
                    {
                        depTable.AddCommitDependency(readTimestamp, version.EndTimestamp, true);
                        return false;
                    }
                    return false;
                }
                else if (status == TxStatus.Committed)
                {
                    return readTimestamp > version.BeginTimestamp &&
                           readTimestamp < txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                }
                else
                {
                    return readTimestamp > version.BeginTimestamp;
                }
            }
        }

        /// <summary>
        /// Given a record's version and a Tx's end timestamp, check this version's visibility.
        /// </summary>
        /// <param name="version">A version entry of a record</param>
        /// <param name="readTimestamp">Tx's end timestamp</param>
        /// <param name="txId">Tx's id.</param>
        /// <param name="txTable">Transaction table.</param>
        /// <returns>True, if the input version is visible to the transaction. False, otherwise.</returns>
        internal bool ValidateVersionVisiblity(VersionEntry version, long readTimestamp, long txId, TransactionTable txTable, ref DependencyTable depTable)
        {
            //case 1: both begin and end fields are timestamp
            //just check whether readTimestamp is in the interval of the version's beginTimestamp and endTimestamp
            if (!version.IsBeginTxId && !version.IsEndTxId)
            {
                return readTimestamp > version.BeginTimestamp && readTimestamp < version.EndTimestamp;
            }
            //case 2: begin field is a TxId, end field is a timestamp
            else if (version.IsBeginTxId && !version.IsEndTxId)
            {
                TxStatus status = txTable.GetTxStatusByTxId(version.BeginTimestamp);
                if (status == TxStatus.Active)
                {
                    return false;
                }
                else if (status == TxStatus.Preparing)
                {
                    if (version.BeginTimestamp == txId)
                    {
                        return true;
                    }
                    else
                    {
                        return readTimestamp > txTable.GetTxEndTimestampByTxId(version.BeginTimestamp) &&
                               readTimestamp < version.EndTimestamp;
                    }
                }
                else if (status == TxStatus.Committed)
                {
                    return readTimestamp > txTable.GetTxEndTimestampByTxId(version.BeginTimestamp) &&
                           readTimestamp < version.EndTimestamp;
                }
                else
                {
                    return false;
                }
            }
            //case 3: begin field is a TxId, end field is a TxId
            else if (version.IsBeginTxId && version.IsEndTxId)
            {
                TxStatus beginTxStatus = txTable.GetTxStatusByTxId(version.BeginTimestamp);
                if (beginTxStatus == TxStatus.Active || beginTxStatus == TxStatus.Aborted)
                {
                    return false;
                }
                else
                {
                    long beginTxEndTimestamp = txTable.GetTxEndTimestampByTxId(version.BeginTimestamp);
                    if (readTimestamp < beginTxEndTimestamp)
                    {
                        return false;
                    }
                    else
                    {
                        TxStatus endTxStatus = txTable.GetTxStatusByTxId(version.EndTimestamp);
                        if (endTxStatus == TxStatus.Active)
                        {
                            return true;
                        }
                        else if (endTxStatus == TxStatus.Preparing)
                        {
                            long endTxEndTimestamp = txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                            if (readTimestamp < endTxEndTimestamp)
                            {
                                return true;
                            }
                            else if (readTimestamp > endTxEndTimestamp)
                            {
                                depTable.AddCommitDependency(txId, version.EndTimestamp, false);
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        else if (endTxStatus == TxStatus.Committed)
                        {
                            return readTimestamp < txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                        }
                        else
                        {
                            return true;
                        }
                    }
                }
            }
            //case 4: begin field is a timestamp, end field is a TxId
            else
            {
                TxStatus status = txTable.GetTxStatusByTxId(version.EndTimestamp);
                if (status == TxStatus.Active)
                {
                    //other transaction can see this old version
                    return readTimestamp > version.BeginTimestamp && readTimestamp != version.EndTimestamp;
                }
                else if (status == TxStatus.Preparing)
                {
                    long txEndTimestamp = txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                    if (readTimestamp < txEndTimestamp)
                    {
                        return true;
                    }
                    else if (readTimestamp > txEndTimestamp)
                    {
                        //speculatively ignore
                        depTable.AddCommitDependency(txId, version.EndTimestamp, false);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else if (status == TxStatus.Committed)
                {
                    return readTimestamp > version.BeginTimestamp &&
                           readTimestamp < txTable.GetTxEndTimestampByTxId(version.EndTimestamp);
                }
                else
                {
                    return readTimestamp > version.BeginTimestamp;
                }
            }
        }

        /// <summary>
        /// Given a record key and a Tx's timestamp, scans the record's version list, 
        /// checks each version's visibility, and returns a version entry visible to the transaction. 
        /// </summary>
        /// <param name="recordKey">The record key</param>
        /// <param name="readTimestamp">The transaction's timestamp</param>
        /// <returns>The version entry visible to the Transaction. Null, if no entry exists.</returns>
        internal VersionEntry ReadVersion(object recordKey, long readTimestamp, TransactionTable txTable, ref DependencyTable depTable)
        {
            return this.GetVersionEntryByTimestamp(recordKey, readTimestamp, txTable, ref depTable);
        }

        /// <summary>
        ///  Given a record (with the key and the payload) and a Tx, inserts the record into the version table.
        /// </summary>
        /// <param name="recordKey">The record key</param>
        /// <param name="record">The record payload</param>
        /// <param name="txId">Transaction Id</param>
        /// <param name="readTimestamp">Transaction's timestamp</param>
        /// <returns>True if the record does not exists in the version table; false; otherwise.</returns>
        internal bool InsertVersion(object recordKey, object record, long txId, long readTimestamp, TransactionTable txTable, ref DependencyTable depTable)
        {
            VersionEntry visibleEntry = this.GetVersionEntryByTimestamp(recordKey, readTimestamp, txTable, ref depTable);

            if (visibleEntry != null)
            {
                return false;
            }

            return this.InsertAndUploadVersion(recordKey,
                new VersionEntry(true, txId, false, long.MaxValue, recordKey, record));
        }

        /// <summary>
        /// Update a version.
        /// Find the version, atomically change it to old version, and insert a new version.
        /// </summary>
        internal bool UpdateVersion(
            object recordKey, 
            object record, 
            long txId, 
            long readTimestamp,
            TransactionTable txTable,
            ref DependencyTable depTable,
            out VersionEntry oldVersion, 
            out VersionEntry newVersion)
        {
            VersionEntry visibleEntry = this.GetVersionEntryByTimestamp(recordKey, readTimestamp, txTable, ref depTable);

            //no version is visible
            if (visibleEntry == null)
            {
                oldVersion = null;
                newVersion = null;
                return false;
            }

            //check the version's updatability
            //the version's end field must be infinity or an aborted transaction
            if (!visibleEntry.IsEndTxId && visibleEntry.EndTimestamp == long.MaxValue ||
                visibleEntry.IsEndTxId && txTable.GetTxStatusByTxId(visibleEntry.EndTimestamp) == TxStatus.Aborted)
            {
                //updatable, two situations:
                //situation 1: the version's begin field is a timestamp, or it is the other transaction's Id
                if (!visibleEntry.IsBeginTxId || visibleEntry.BeginTimestamp != txId)
                {
                    oldVersion = visibleEntry;
                    //(1) ATOMICALLY set the version's end timestamp to TxId, make it an old version.
                    if (this.UpdateAndUploadVersion(recordKey, visibleEntry.VersionKey, visibleEntry,
                        new VersionEntry(
                            visibleEntry.IsBeginTxId,
                            visibleEntry.BeginTimestamp,
                            true,
                            txId,
                            visibleEntry.RecordKey,
                            visibleEntry.Record)))
                    {
                        //if (1) success, (2) insert a new version
                        newVersion = new VersionEntry(true, txId, false, long.MaxValue, recordKey, record);
                        return this.InsertAndUploadVersion(recordKey, newVersion);
                    }
                    else
                    {
                        //if (1) failed, other transaction has already set the version's end field, can not update.
                        newVersion = null;
                        return false;
                    }
                }
                //case 2: if the version's begin field has the same transaction Id
                else
                {
                    oldVersion = null;
                    newVersion = new VersionEntry(
                        visibleEntry.IsBeginTxId,
                        visibleEntry.BeginTimestamp,
                        visibleEntry.IsEndTxId,
                        visibleEntry.EndTimestamp,
                        recordKey,
                        record);
                    //change the record directly on this version
                    return this.UpdateAndUploadVersion(recordKey, visibleEntry.VersionKey, visibleEntry, newVersion);
                }
            }
            //a version is visible but not updatable, can not perform update, return false
            else
            {
                oldVersion = null;
                newVersion = null;
                return false;
            }
        }

        /// <summary>
        /// Find and delete a version.
        /// </summary>
        internal bool DeleteVersion(
            object recordKey,
            long txId,
            long readTimestamp,
            TransactionTable txTable,
            ref DependencyTable depTable,
            out VersionEntry deletedVersion)
        {
            VersionEntry visibleEntry = this.GetVersionEntryByTimestamp(recordKey, readTimestamp, txTable, ref depTable);

            //no version is visible
            if (visibleEntry == null)
            {
                deletedVersion = null;
                return false;
            }

            //check the version's deletability
            if (!visibleEntry.IsEndTxId && visibleEntry.EndTimestamp == long.MaxValue ||
                visibleEntry.IsEndTxId && txTable.GetTxStatusByTxId(visibleEntry.EndTimestamp) == TxStatus.Aborted)
            {
                deletedVersion = visibleEntry;
                //deletable, two case:
                //case 1: the version's begin field is a timestamp, or it is the other transaction's Id
                if (!visibleEntry.IsBeginTxId || visibleEntry.BeginTimestamp != txId)
                {
                    //(1) ATOMICALLY set the version's end timestamp to TxId
                    return this.UpdateAndUploadVersion(recordKey, visibleEntry.VersionKey, visibleEntry,
                            new VersionEntry(
                            visibleEntry.IsBeginTxId,
                            visibleEntry.BeginTimestamp,
                            true,
                            txId,
                            visibleEntry.RecordKey,
                            visibleEntry.Record));
                }
                //case 2: if the version's begin field has the same transaction Id
                //this version is created by the same transaction, and the transaction want to delete it.
                //Delete this version entry directly.
                else
                {
                    return this.DeleteVersionEntry(recordKey, visibleEntry.VersionKey);
                }
            }
            //a version is visible but not deletable, can not perform delete, return false
            else
            {
                deletedVersion = null;
                return false;
            }
        }

        /// <summary>
        /// Check visibility of the version read before, used in validation phase.
        /// Given a record's recordKey, the version's versionKey, the current readTimestamp (tx's end timestamp), and the transaction Id
        /// First find the version then check whether it is stil visible. 
        /// </summary>
        internal bool CheckReadVisibility(
            object recordKey, 
            long versionKey, 
            long txEndTimestamp,
            long txId,
            TransactionTable txTable,
            ref DependencyTable depTable)
        {
            VersionEntry versionEntry = this.GetVersionEntryByKey(recordKey, versionKey);
            return versionEntry != null && this.ValidateVersionVisiblity(versionEntry, txEndTimestamp, txId, txTable, ref depTable);
        }

        /// <summary>
        /// Check for Phantom (key phantom) of a scan.
        /// </summary>
        internal bool CheckPhantom(object recordKey, long oldScanTime, long newScanTime, long txId, TransactionTable txTable)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// After a transaction T commit,
        /// given the recordKey and the versionKey,
        /// propagate T's end timestamp to the Begin and End fields of new and old versions
        /// </summary>
        internal void UpdateCommittedVersionTimestamp(
            object recordKey,
            long versionKey,
            long txId, 
            long endTimestamp)
        {
            VersionEntry version = this.GetVersionEntryByKey(recordKey, versionKey);

            if (version != null)
            {
                VersionEntry commitedVersion = version;
                if (version.IsBeginTxId && version.BeginTimestamp == txId)
                {
                    commitedVersion.IsBeginTxId = false;
                    commitedVersion.BeginTimestamp = endTimestamp;
                }

                if (version.IsEndTxId && version.EndTimestamp == txId)
                {
                    commitedVersion.IsEndTxId = false;
                    commitedVersion.EndTimestamp = endTimestamp;
                }

                this.UpdateAndUploadVersion(recordKey, versionKey, version, commitedVersion);
            }
        }

        /// <summary>
        /// After a transaction T abort,
        /// given the recordKey and the versionKey,
        /// T sets the Begin field of its new versions to infinity, thereby making them invisible to all transactions,
        /// and try to reset the End fields of its old versions to infinity.
        /// </summary>
        internal void UpdateAbortedVersionTimestamp(object recordKey, long versionKey, long txId)
        {
            VersionEntry version = this.GetVersionEntryByKey(recordKey, versionKey);

            if (version != null)
            {
                //new version
                if (version.IsBeginTxId && version.BeginTimestamp == txId)
                {
                    this.UpdateAndUploadVersion(recordKey, versionKey, version,
                        new VersionEntry(
                            false,
                            long.MaxValue,
                            version.IsEndTxId,
                            version.EndTimestamp,
                            version.RecordKey,
                            version.Record));
                }
                //old version
                else if (version.IsEndTxId && version.EndTimestamp == txId)
                {
                    this.UpdateAndUploadVersion(recordKey, versionKey, version,
                        new VersionEntry(
                            version.IsBeginTxId,
                            version.BeginTimestamp,
                            false,
                            long.MaxValue,
                            version.RecordKey,
                            version.Record));
                }
            }
        }
    }

    public abstract partial class VersionTable : IVersionedTableStore
    {
        public abstract bool DeleteJson(object key, Transaction tx);
        public abstract JObject GetJson(object key, Transaction tx);
        public abstract IList<JObject> GetRangeJsons(object lowerKey, object upperKey, Transaction tx);
        public abstract IList<object> GetRangeRecordKeyList(object lowerValue, object upperValue, Transaction tx);
        public abstract IList<object> GetRecordKeyList(object value, Transaction tx);
        public abstract bool InsertJson(object key, JObject record, Transaction tx);
        public abstract bool UpdateJson(object key, JObject record, Transaction tx);
    }
}