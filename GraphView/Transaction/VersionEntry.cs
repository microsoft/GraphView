namespace GraphView.Transaction
{
    using Newtonsoft.Json.Linq;
    using System.Runtime.Serialization;
    using System;

    [Serializable]
    internal class VersionEntry : ISerializable, ICloneable
    {
        private readonly object recordKey;
        private readonly long versionKey;
        private long beginTimestamp;
        private long endTimestamp;
        private readonly object record;
        private long txId;
        private long maxCommitTs;

        public object RecordKey
        {
            get
            {
                return this.recordKey;
            }
        }

        public long VersionKey
        {
            get
            {
                return this.versionKey;
            }
        }

        public object Record
        {
            get
            {
                return this.record;
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

        public long BeginTimestamp
        {
            get
            {
                return this.beginTimestamp;
            }
            set
            {
                this.beginTimestamp = value;
            }
        }

        public long EndTimestamp
        {
            get
            {
                return this.endTimestamp;
            }
            set
            {
                this.endTimestamp = value;
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

        public JObject JsonRecord
        {
            get
            {
                return (JObject)this.record;
            }
        }

        public VersionEntry(
            object recordKey,
            long versionKey,
            object record,
            long txId,
            long beginTimestamp,
            long endTimestamp,
            long maxCommitTs)
        {
            this.recordKey = recordKey;
            this.versionKey = versionKey;
            this.record = record;
            this.txId = txId;
            this.beginTimestamp = beginTimestamp;
            this.endTimestamp = endTimestamp;
            this.maxCommitTs = maxCommitTs;
        } 

        public VersionEntry(SerializationInfo info, StreamingContext context)
        {
            this.recordKey = info.GetValue("recordKey", typeof(object));
            this.versionKey = (long) info.GetValue("versionKey", typeof(long));
            this.beginTimestamp = (long)info.GetValue("beginTimestamp", typeof(long));
            this.endTimestamp = (long)info.GetValue("endTimestamp", typeof(long));
            this.txId = (long)info.GetValue("txId", typeof(long));
            this.maxCommitTs = (long)info.GetValue("maxCommitTs", typeof(long));
            this.record = info.GetValue("record", typeof(long));
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

        public bool ContentEqual(VersionEntry other)
        {
            if (other == null)
            {
                return false;
            }

            return this.versionKey == other.VersionKey &&
                   this.txId == other.TxId &&
                   this.beginTimestamp == other.BeginTimestamp &&
                   this.endTimestamp == other.EndTimestamp &&
                   this.maxCommitTs == other.MaxCommitTs;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("recordKey", this.recordKey, typeof(object));
            info.AddValue("versionKey", this.versionKey, typeof(long));
            info.AddValue("beginTimestamp", this.beginTimestamp, typeof(long));
            info.AddValue("endTimestamp", this.endTimestamp, typeof(long));
            info.AddValue("txId", this.txId, typeof(long));
            info.AddValue("record", this.record, typeof(object));
            info.AddValue("maxCommitTs", this.maxCommitTs, typeof(long));
        }

        public object Clone()
        {
            return new VersionEntry(this.recordKey, this.versionKey, this.record,
                this.txId, this.beginTimestamp, this.endTimestamp, this.maxCommitTs);
        }
    }
}
