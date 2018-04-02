
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
            throw new NotImplementedException();
        }

        internal virtual bool CheckVisibility(VersionEntry versionEntry)
        {
            throw new NotImplementedException();
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

        internal virtual VersionEntry ReadAndInitialize(object recordKey, out long largestVersionKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// It will be called when the postprocessing is ongoing. In general, we only need
        /// to replace the beginTimestamp and endTimestamp, and no need to update record.
        /// 
        /// In case of some other storages, we must replace the whole [begin, end, record] even 
        /// we just want to change begin and end. Thus, here we also put the record in param list
        /// to keep the interface extensiable.
        /// </summary>
        /// <returns></returns>
        internal virtual long ReplaceVersionEntry(object recordKey, long versionKey, long beginTimestamp, long endTimestamp, long txId, long readTxId) 
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Upload a new version entry when insert or update a version
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="versionEntry"></param>
        /// <returns></returns>
        internal virtual bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            throw new NotImplementedException();
        }

        internal virtual VersionEntry UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTime)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }
    }
}