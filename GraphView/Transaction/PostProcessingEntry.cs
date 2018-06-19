namespace GraphView.Transaction
{
    using NonBlocking;
    using System.Collections.Generic;

    class PostProcessingEntry : TxSetEntry
    {
        internal long VersionKey { get; set; }
        internal long BeginTimestamp { get; set; }
        internal long EndTimestamp { get; set; }
        internal VersionEntry RemoteVerEntry { get; set; }
        internal ConcurrentDictionary<long, VersionEntry> RemoteVerList { get; set; }

        public PostProcessingEntry()
        {

        }

        public PostProcessingEntry(long versionKey, long beginTimestamp, long endTimestamp)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
        }

        public PostProcessingEntry(
            string tableId, 
            object recordKey, 
            long versionKey, 
            long beginTimestamp, 
            long endTimestamp) : base(tableId, recordKey)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
        }

        public PostProcessingEntry(
           string tableId,
           object recordKey,
           long versionKey,
           long beginTimestamp,
           long endTimestamp,
           VersionEntry remoteVerEntry,
           IDictionary<long, VersionEntry> remoteVerList) : base(tableId, recordKey)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.RemoteVerEntry = remoteVerEntry;
            this.RemoteVerList = RemoteVerList;
        }

        public void Set(
           string tableId,
           object recordKey,
           long versionKey,
           long beginTimestamp,
           long endTimestamp,
           VersionEntry remoteVerEntry,
           IDictionary<long, VersionEntry> remoteVerList)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.RemoteVerEntry = remoteVerEntry;
            this.RemoteVerList = RemoteVerList;
        }

        public override bool Equals(object obj)
        {
            if (!base.Equals(obj))
            {
                return false;
            }

            PostProcessingEntry other = obj as PostProcessingEntry;
            if (other == null)
            {
                return false;
            }

            return this.VersionKey == other.VersionKey &&
                this.BeginTimestamp == other.BeginTimestamp &&
                this.EndTimestamp == other.EndTimestamp;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + base.GetHashCode();
            hash = hash * 23 + this.VersionKey.GetHashCode();
            hash = hash * 23 + this.BeginTimestamp.GetHashCode();
            hash = hash * 23 + this.EndTimestamp.GetHashCode();
            return hash;
        }
    }
}
