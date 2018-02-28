using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json.Linq;

namespace GraphView.Transaction
{
    /// <summary>
    /// A version table for concurrency control.
    /// </summary>
    public abstract class VersionTable
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

        internal virtual IEnumerable<VersionEntry> GetVersionList(object recordKey, long timestamp)
        {
            return this.GetVersionList(recordKey);
        }

        internal virtual void InsertAndUploadVersion(object recordKey, VersionEntry version)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateAndUploadVersion(object recordKey, VersionEntry oldVersion, VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual void DeleteVersionEntry(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Given a record's version and a Tx's timestamp (or a TxId), check this version's visibility.
        /// </summary>
        /// <param name="version">A version entry of a record</param>
        /// <param name="readTimestamp">Tx's timestamp</param>
        /// <returns>True, if the input version is visible to the transaction. False, otherwise.</returns>
        internal bool CheckVersionVisibility(VersionEntry version, long readTimestamp)
        {
            //case 1: both begin and end fields are timestamp
            //just checl whether readTimestamp is in the interval of the version's beginTimestamp and endTimestamp 
            if (!version.IsBeginTxId && !version.IsEndTxId)
            {
                return readTimestamp > version.BeginTimestamp && readTimestamp < version.EndTimestamp;
            }
            //case 2: begin field is a TxId, end field is a timestamp
            //just check whether this version is created by the same transaction
            else if (version.IsBeginTxId && !version.IsEndTxId)
            {
                return version.BeginTimestamp == readTimestamp;
            }
            //case 3: begin field is a TxId, end field is a TxId
            //this must must be deleted by the same transaction, not visible
            else if (version.IsBeginTxId && version.IsEndTxId)
            {
                return false;
            }
            //case 4: begin field is a timestamp, end field is a TxId
            //first check whether the readTimestamp > version's beginTimestamp
            //then, check the version's end field
            else
            {
                if (readTimestamp > version.BeginTimestamp)
                {
                    //this is the old version deleted by the same Transaction
                    if (version.EndTimestamp == readTimestamp)
                    {
                        return false;
                    }
                    //other transaction can see this old version
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    return false;
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
        internal VersionEntry ReadVersion(object recordKey, long readTimestamp)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey, readTimestamp);

            if (versionList == null)
            {
                return null;
            }

            foreach (VersionEntry version in versionList)
            {
                if (this.CheckVersionVisibility(version, readTimestamp))
                {
                    return version;
                }
            }

            return null;
        }

        /// <summary>
        ///  Given a record (with the key and the payload) and a Tx, inserts the record into the version table.
        /// </summary>
        /// <param name="recordKey">The record key</param>
        /// <param name="record">The record payload</param>
        /// <param name="txId">Transaction Id</param>
        /// <param name="readTimestamp">Transaction's timestamp</param>
        /// <returns>True if the record does not exists in the version table; false; otherwise.</returns>
        internal bool InsertVersion(object recordKey, JObject record, long txId, long readTimestamp)
        {
            VersionEntry visibleEntry = this.ReadVersion(recordKey, readTimestamp);

            if (visibleEntry != null)
            {
                return false;
            }

            this.InsertAndUploadVersion(recordKey, new VersionEntry(true, txId, false, long.MaxValue, recordKey, record));
            return true;
        }

        /// <summary>
        /// Update a version.
        /// Find the version, atomically change it to old version, and insert a new version.
        /// </summary>
        internal bool UpdateVersion(
            object recordKey, 
            JObject record, 
            long txId, 
            long readTimestamp, 
            out VersionEntry oldVersion, 
            out VersionEntry newVersion)
        {
            VersionEntry visibleEntry = this.ReadVersion(recordKey, readTimestamp);

            //no version is visible
            if (visibleEntry == null)
            {
                oldVersion = null;
                newVersion = null;
                return false;
            }

            //check the version's updatability
            if (!visibleEntry.IsEndTxId && visibleEntry.EndTimestamp == long.MaxValue)
            {
                //updatable, two case:
                //case 1: the version's begin field is a timestamp
                if (!visibleEntry.IsBeginTxId)
                {
                    oldVersion = visibleEntry;
                    //(1) ATOMICALLY set the version's end timestamp to TxId, make it an old version.
                    if (this.UpdateAndUploadVersion(recordKey, visibleEntry,
                        new VersionEntry(
                            visibleEntry.IsBeginTxId,
                            visibleEntry.BeginTimestamp,
                            true,
                            txId,
                            visibleEntry.RecordKey,
                            visibleEntry.Record)))
                    {
                        //if (1) success, insert a new version
                        newVersion = new VersionEntry(true, txId, false, long.MaxValue, recordKey, record);
                        this.InsertAndUploadVersion(recordKey, newVersion);
                        return true;
                    }
                    else
                    {
                        //if (1) failed, other transaction has already set the version's end field, can not update.
                        newVersion = null;
                        return false;
                    }
                }
                //case 2: if the version's begin field is a TxId
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
                    this.UpdateAndUploadVersion(recordKey, visibleEntry, newVersion);
                    return true;
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
            out VersionEntry deletedVersion)
        {
            VersionEntry visibleEntry = this.ReadVersion(recordKey, readTimestamp);

            //no version is visible
            if (visibleEntry == null)
            {
                deletedVersion = null;
                return false;
            }

            //check the version's deletability
            if (!visibleEntry.IsEndTxId && visibleEntry.EndTimestamp == long.MaxValue)
            {
                deletedVersion = visibleEntry;
                //deletable, two case:
                //case 1: the version's begin field is a timestamp
                if (!visibleEntry.IsBeginTxId)
                {
                    //(1) ATOMICALLY set the version's end timestamp to TxId
                    if (this.UpdateAndUploadVersion(recordKey, visibleEntry,
                        new VersionEntry(
                            visibleEntry.IsBeginTxId,
                            visibleEntry.BeginTimestamp,
                            true,
                            txId,
                            visibleEntry.RecordKey,
                            visibleEntry.Record)))
                    {
                        //if (1) success, delete successfully
                        return true;
                    }
                    else
                    {
                        //if (1) failed, other transaction has already set the version's end field, can not delete
                        return false;
                    }
                }
                //case 2: if the version's begin field is a TxId
                //this version is created by the same transaction, and the transaction want to delete it.
                //Delete this version entry directly.
                else
                {
                    this.DeleteVersionEntry(recordKey, visibleEntry.VersionKey);
                    return true;
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
        /// Given a record's recordKey, the version's beginTimestamp, the current readTimestamp, and the transaction Id
        /// First find the version then check whether it is stil visible. 
        /// </summary>
        internal bool CheckReadVisibility(
            object recordKey, 
            long readVersionBeginTimestamp, 
            long readTimestamp,
            long txId)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);

            if (versionList == null)
            {
                return false;
            }

            foreach (VersionEntry version in versionList)
            {
                //case 1: the versin's begin field is a TxId, and this Id equals to the transaction's Id
                if (version.IsBeginTxId && version.BeginTimestamp == txId)
                {
                    //check the visibility of this version, using the transaction's endTimestamp
                    if (this.CheckVersionVisibility(version, readTimestamp))
                    {
                        return true;
                    }
                    else
                    {
                        continue;
                    }
                }
                //case 2: the version's begin field is a timestamp, and this timestamp equals to the read version's begin timestamp
                else if (!version.IsBeginTxId && version.BeginTimestamp == readVersionBeginTimestamp)
                {
                    return this.CheckVersionVisibility(version, readTimestamp);
                }
            }

            return false;
        }

        /// <summary>
        /// Check for Phantom of a scan.
        /// Only check for version phantom currently. Check key phantom is NOT implemented.
        /// Look for versions that came into existence during T’s lifetime and are visible as of the end of the transaction.
        /// </summary>
        internal bool CheckPhantom(object recordKey, long oldScanTime, long newScanTime)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);

            if (versionList == null)
            {
                return true;
            }

            foreach (VersionEntry version in versionList)
            {
                if (!version.IsBeginTxId)
                {
                    if (version.BeginTimestamp > oldScanTime && version.BeginTimestamp < newScanTime)
                    {
                        if (this.CheckVersionVisibility(version, newScanTime))
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// After a transaction T commit,
        /// propagate a T's end timestamp to the Begin and End fields of new and old versions
        /// </summary>
        internal void UpdateCommittedVersionTimestamp(
            object recordKey, 
            long txId, 
            long endTimestamp)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);

            if (versionList != null)
            {
                //tranverse the whole version list and seach for versions that were modified by the transaction
                foreach (VersionEntry version in versionList)
                {
                    if (version.IsBeginTxId && version.BeginTimestamp == txId ||
                        version.IsEndTxId && version.EndTimestamp == txId)
                    {
                        VersionEntry commitedVersion = version;
                        if (version.IsBeginTxId)
                        {
                            commitedVersion.IsBeginTxId = false;
                            commitedVersion.BeginTimestamp = endTimestamp;
                        }

                        if (version.IsEndTxId)
                        {
                            commitedVersion.IsEndTxId = false;
                            commitedVersion.EndTimestamp = endTimestamp;
                        }

                        this.UpdateAndUploadVersion(recordKey, version, commitedVersion);
                    }
                }
            }
        }

        /// <summary>
        /// After a transaction T abort,
        /// T sets the Begin field of its new versions to infinity, thereby making them invisible to all transactions,
        /// and reset the End fields of its old versions to infinity.
        /// </summary>
        internal void UpdateAbortedVersionTimestamp(object recordKey, long txId)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);

            if (versionList != null)
            {
                //tranverse the whole version list and seach for versions that were modified by the transaction
                foreach (VersionEntry version in versionList)
                {
                    //new version
                    if (version.IsBeginTxId && version.BeginTimestamp == txId)
                    {
                        this.UpdateAndUploadVersion(recordKey, version,
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
                        this.UpdateAndUploadVersion(recordKey, version,
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
    }

}