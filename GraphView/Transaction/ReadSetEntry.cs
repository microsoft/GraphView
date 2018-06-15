namespace GraphView.Transaction
{
    using NonBlocking;
    using System.Collections.Generic;

    internal class ReadSetEntry : TxSetEntry
    {
        internal long VersionKey { get; set; }
        internal long BeginTimestamp { get; set; }
        internal long EndTimestamp { get; set; }
        internal long TxId { get; set; }
        internal object Record { get; set; }
        internal long TailKey { get; set; }
        internal VersionEntry RemoteVerEntry { get; set; }
        internal IDictionary<long, VersionEntry> RemoteVerList { get; set; }

        public ReadSetEntry()
        {

        }

        public ReadSetEntry(long versionKey, long beginTimestamp, long endTimestamp, long txId, object record)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.TxId = txId;
            this.Record = record;
        }

        public ReadSetEntry(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            long txId,
            object record,
            long tailKey) : base(tableId, recordKey)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.TxId = txId;
            this.Record = record;
            this.TailKey = tailKey;
        }

        public ReadSetEntry(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            long txId,
            object record,
            long tailKey,
            VersionEntry remoteVerEntry = null,
            ConcurrentDictionary<long, VersionEntry> remoteVerList = null) : base(tableId, recordKey)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.TxId = txId;
            this.Record = record;
            this.TailKey = tailKey;
            this.RemoteVerEntry = remoteVerEntry;
            this.RemoteVerList = remoteVerList;
        }

        public void Set(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            long txId,
            object record,
            long tailKey,
            VersionEntry remoteVerEntry = null,
            IDictionary<long, VersionEntry> remoteVerList = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.TxId = txId;
            this.Record = record;
            this.TailKey = tailKey;
            this.RemoteVerEntry = remoteVerEntry;
            this.RemoteVerList = remoteVerList;
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
