
namespace GraphView.Transaction
{
    using System;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A version Db for concurrency control.
    /// </summary>
    public abstract class VersionDb
    {
        internal virtual VersionEntry ReadVersion(
            string tableId,
            object recordKey,
            long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool InsertVersion(
            string tableId,
            object recordKey,
            JObject record,
            long txId,
            long readTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual bool DeleteVersion(
            string tableId,
            object recordKey,
            long txId,
            long readTimestamp,
            out VersionEntry deletedVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool UpdateVersion(
            string tableId,
            object recordKey,
            JObject record,
            long txId,
            long readTimestamp,
            out VersionEntry oldVersion,
            out VersionEntry newVersion)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckReadVisibility(
            string tableId,
            object recordKey,
            long readVersionBeginTimestamp,
            long readTimestamp,
            long txId)
        {
            throw new NotImplementedException();
        }

        internal virtual bool CheckPhantom(
            string tableId,
            object recordKey,
            long oldScanTime,
            long newScanTime)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateCommittedVersionTimestamp(
            string tableId,
            object recordKey,
            long txId,
            long endTimestamp)
        {
            throw new NotImplementedException();
        }

        internal virtual void UpdateAbortedVersionTimestamp(
            string tableId,
            object recordKey,
            long txId)
        {
            throw new NotImplementedException();
        }
    }
}
