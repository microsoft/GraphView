using System.Runtime.CompilerServices;

namespace GraphView.Transaction
{
    using Newtonsoft.Json.Linq;
    using System.Runtime.Serialization;
    using System;

    [Serializable]
    internal class VersionEntry : ISerializable, ICloneable
    {
        private readonly Payload payload;
        private long txId;
        private long maxCommitTs;

        public object RecordKey
        {
            get
            {
                return this.payload.RecordKey;
            }
        }

        public long VersionKey
        {
            get
            {
                return this.payload.VersionKey;
            }
        }

        public long TxId
        {
            get
            {
                return this.txId;
            }
            set
            {
                this.txId = value;
            }
        }

        public long MaxCommitTs
        {
            get
            {
                return this.maxCommitTs;
            }
            set
            {
                this.maxCommitTs = value;
            }
        }

        public VersionEntry(
            Payload payload,
            long txId,
            long maxCommitTs)
        {
            this.payload = payload;
            this.txId = txId;
            this.maxCommitTs = maxCommitTs;
        } 

        public VersionEntry(SerializationInfo info, StreamingContext context)
        {
            this.payload = (Payload)info.GetValue("payload", typeof(Payload));
            this.txId = (long)info.GetValue("txId", typeof(long));
            this.maxCommitTs = (long)info.GetValue("maxCommitTs", typeof(long));
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + this.VersionKey.GetHashCode();
            hash = hash * 23 + this.RecordKey.GetHashCode();

            return hash;
        }

        public override bool Equals(object obj)
        {
            VersionEntry ventry = obj as VersionEntry;
            if (ventry == null)
            {
                return false;
            }

            return this.VersionKey == ventry.VersionKey &&
                this.RecordKey == ventry.RecordKey;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("payload", this.payload, typeof(Payload));
            info.AddValue("txId", this.txId, typeof(long));
            info.AddValue("maxCommitTs", this.maxCommitTs, typeof(long));
        }

        public object Clone()
        {
            return new VersionEntry(this.payload, this.txId, this.maxCommitTs);
        }
    }
}
