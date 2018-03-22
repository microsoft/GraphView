
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

        internal virtual VersionEntry GetRecentVersionEntry(object recordKey)
        {
            throw new NotImplementedException();
        }

        internal virtual VersionEntry GetVersionEntryByKey(object recordKey, long versionKey)
        {
            throw new NotImplementedException();
        }

        internal virtual bool InsertAndUploadVersion(object recordKey, VersionEntry version)
        {
            throw new NotImplementedException();
        }

        // include SetMaxCommitTs inside this function 
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