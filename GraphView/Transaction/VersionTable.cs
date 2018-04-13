using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

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
        /// Get a list of version entries, which will be used to check visiablity
        /// </summary>
        /// <returns></returns>
        internal virtual IEnumerable<VersionEntry> GetVersionList(object recordKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// To keep the same actions whether the version list is empty or not when insert a new version
        /// That is computing the new version's version key as largestKey + 1
        /// We would try to add an useless version entry at the head of version list if it's empty
        /// InitializeAndGetVersionList has two steps:
        /// (1) initialize a version list with adding an empty version if the version list is empty
        /// (2) read all version entries inside the version list
        /// </summary>
        /// <returns>An IEnumerable of version entries</returns>
        internal virtual IEnumerable<VersionEntry> InitializeAndGetVersionList(object recordKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// This method will be called during Uploading Phase and the PostProcessing Phase.
        /// </summary>
        /// <returns></returns>
        internal virtual VersionEntry ReplaceVersionEntry(object recordKey, long versionKey, 
            long beginTimestamp, long endTimestamp, long txId, long readTxId, long expectedEndTimestamp) 
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Upload a new version entry when insert or update a version
        /// </summary>
        /// <returns>True of False</returns>
        internal virtual bool UploadNewVersionEntry(object recordKey, long versionKey, VersionEntry versionEntry)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// In negotiation phase, if no another transaction is updating tx, make sure future tx's who
        /// updates x have CommitTs greater than or equal to the commitTime of current transaction
        /// 
        /// <returns></returns>
        /// Update the version's maxCommitTs in the validataion phase
        /// </summary>
        /// <param name="commitTs">The current transaction's commit time</param>
        /// </param>
        /// <returns>A updated or non-updated version entry</returns>
        internal virtual VersionEntry UpdateVersionMaxCommitTs(object recordKey, long versionKey, long commitTs)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// In the Validate phase, get this version entry to check its MaxCommitTs.
        /// </summary>
        /// <param name="recordKey"></param>
        /// <param name="versionKey"></param>
        /// <returns>The current version entry</returns>
        internal virtual VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Delete a version entry by recordKey and version Key
        /// It will be called when the insertion or update is aborted.
        /// Inserted new version will be deleted to avoid unnecessary write conflicts
        /// </summary>
        /// <returns></returns>
        internal virtual bool DeleteVersionEntry(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }
    }
}