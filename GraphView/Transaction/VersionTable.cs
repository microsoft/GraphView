
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

        internal virtual long ReplaceVersionEntry(object recordKey, long versionKey, long txId)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UploadVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UploadPayload(object recordKey, long versionKey, Payload payload)
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