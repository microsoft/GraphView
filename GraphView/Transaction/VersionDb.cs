
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using RecordRuntime;

    public abstract partial class VersionDb
    {
        internal virtual VersionTable GetVersionTable(string tableId)
        {
            throw new NotImplementedException();
        }

        internal virtual VersionTable CreateVersionTable(string tableId)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteTable(string tableId)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// A version Db for concurrency control.
    /// </summary>
    public abstract partial class VersionDb
    {
        internal virtual VersionEntry GetRecentVersionEntry(string tableId, object recordKey, out long largestVersionKey)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                largestVersionKey = -1;
                return null;
            }

            return versionTable.GetRecentVersionEntry(recordKey, out largestVersionKey);
        }

        internal virtual VersionEntry GetVersionEntryByKey(string tableId, object recordKey, long versionKey)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return null;
            }

            return versionTable.GetVersionEntryByKey(recordKey, versionKey);
        }

        /// <summary>
        /// In negotiation phase, if no another transaction is updating tx, make sure future tx's who
        /// update x have CommitTs greater than the commitTime of current transaction
        /// </summary>
        /// <returns></returns>
        internal virtual VersionEntry UpdateVersionMaxCommitTs(string tableId, object recordKey, long versionKey, long commitTime)
        {
            VersionTable versionTable = this.GetVersionTable(tableId);
            if (versionTable == null)
            {
                return null;
            }

            return versionTable.UpdateVersionMaxCommitTs(recordKey, versionKey, commitTime);
        }
    }

    public abstract partial class VersionDb
    {
        /// <summary>
        /// Try different random txIds and return the txId if successes
        /// </summary>
        /// <returns></returns>
        internal virtual long InsertNewTx()
        {
            throw new NotImplementedException();
        }

        internal virtual TxTableEntry GetTxTableEntry(long txId)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateTxStatus(long txId, TxStatus status)
        {
            throw new NotImplementedException();
        }

        internal virtual long GetAndSetCommitTime(long txId, long lowerBound)
        {
            throw new NotImplementedException();
        }

        internal virtual long UpdateCommitLowerBound(long txId, long commitTs)
        {
            throw new NotImplementedException();
        }
    }
}
