namespace GraphView.Transaction
{
    using System.Collections.Generic;

    class VersionKeyEntry : TxSetEntry
    {
        internal long VersionKey;

        internal IDictionary<long, VersionEntry> RemoteVerList { get; set; }

        public VersionKeyEntry()
        {

        }

        public VersionKeyEntry(string tableId, object recordKey, long versionKey) :
            base(tableId, recordKey)
        {
            this.VersionKey = versionKey;
        }

        public void Set(
            string tableId, 
            object recordKey, 
            long versionKey,
            IDictionary<long, VersionEntry> remoteVerList = null)
        {
            this.TableId = tableId;
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
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
