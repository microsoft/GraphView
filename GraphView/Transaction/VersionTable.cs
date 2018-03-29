
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

        internal virtual IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get the most recent commited version entry
        /// </summary>
        /// <param name="recordKey"></param>
        /// <returns></returns>
        internal virtual VersionEntry GetRecentVersionEntry(object recordKey, out long largestVersionKey)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);
            largestVersionKey = -1;
            foreach (VersionEntry entry in versionList)
            {
                largestVersionKey = largestVersionKey < entry.VersionKey ? entry.VersionKey : largestVersionKey;
                if ((entry.EndTimestamp == long.MaxValue && entry.TxId == -1) ||
                    (entry.EndTimestamp != long.MaxValue && entry.TxId != -1))
                {
                    largestVersionKey = entry.VersionKey;
                    return entry;
                }
            }
            return null;
        }
                                                                 
        internal virtual VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            IEnumerable<VersionEntry> versionList = this.GetVersionList(recordKey);
            foreach (VersionEntry entry in versionList)
            {
                if (entry.VersionKey == versionKey)
                {
                    return entry;
                }
            }
            return null;
        }

        internal virtual bool UpdateVersionMaxCommitTs(object recordKey, long versionKey, VersionEntry versionEntry, long commitTime)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// To ensure the insertion works well in critical section, we use some shared variables to simulate locks.
        /// Here we take the previous versionKey as the shared variable. 
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="version"></param>
        /// <param name="preVersion">To en</param>
        /// <returns></returns>
        internal virtual bool InsertAndUploadVersion(object recordKey, VersionEntry version, long preVersionKey)
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
        //this method integrate the InsertAndUploadVersion() and UpdateAndUploadVersion().
        internal bool UploadRecordByKey(object recordKey, VersionEntry oldVersion, VersionEntry newVersion)
        {
            if (oldVersion == null)
            {
                //just insert
                return this.InsertAndUploadVersion(recordKey, newVersion, newVersion.VersionKey - 1);
            }
            //upload the old version when update
            return this.UpdateAndUploadVersion(recordKey, oldVersion.VersionKey, oldVersion, newVersion);
        }

        //change the the committed version's begin/end Ts from txId to tx's commitTs.
        internal void UpdateCommittedVersionTimestamp(object recordKey, long versionKey, long commitTimestamp, long txId, bool isOld)
        {
            throw new NotImplementedException();
        }

        internal void UpdateAbortedVersionTimestamp(object recordKey, long versionKey, long txId, bool isOld)
        {
            throw new NotImplementedException();
        }
    }
}