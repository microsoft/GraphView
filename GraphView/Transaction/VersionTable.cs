
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
        internal virtual VersionEntry GetRecentVersionEntry(object recordKey)
        {
            throw new NotImplementedException();
        }

        internal virtual VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// In negotiation phase, if no another transaction is updating tx, make sure future tx's who
        /// update x have CommitTs greater than the commitTime of current transaction
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <param name="commitTime">Current transaction's commit time</param>
        /// <returns></returns>
        internal virtual bool UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTime)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// To ensure the insertion works well in critical section, we use some shared variables to simulate locks.
        /// Here we take the previous version as the shared variable. 
        /// (1) If it's an insertion operation from the upper level, preVersion should be a version entry with the maximum version Key
        /// (2) If it's a update operation, preVersion should be the old version who has been modified by Tx
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="version"></param>
        /// <param name="preVersion">To en</param>
        /// <returns></returns>
        internal virtual bool InsertAndUploadVersion(object recordKey, VersionEntry version, VersionEntry preVersion)
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
}