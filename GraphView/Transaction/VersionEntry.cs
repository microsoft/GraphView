namespace GraphView.Transaction
{
    using Newtonsoft.Json.Linq;
    using System.Runtime.Serialization;
    using System;

    [Serializable]
    internal class VersionEntry : ISerializable
    {
        private bool isBeginTxId;
        private long beginTimestamp;
        private bool isEndTxId;
        private long endTimestamp;
        private readonly object record;

        private readonly object recordKey;
        private readonly long versionKey;

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

        public bool IsBeginTxId
        {
            get
            {
                return this.isBeginTxId;
            }
            set
            {
                this.isBeginTxId = value;
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

        public bool IsEndTxId
        {
            get
            {
                return this.isEndTxId;
            }
            set
            {
                this.isEndTxId = value;
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

        public object Record
        {
            get
            {
                return this.record;
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
            bool isBeginTxId, 
            long beginTimestamp, 
            bool isEndTxId, 
            long endTimestamp, 
            object recordKey, 
            object record)
        {
            this.isBeginTxId = isBeginTxId;
            this.beginTimestamp = beginTimestamp;
            this.isEndTxId = isEndTxId;
            this.endTimestamp = endTimestamp;
            this.record = record;

            this.recordKey = recordKey;
            this.versionKey = beginTimestamp;
        }

        // The constructor is used to reconstruct object from serialized values
        public VersionEntry(
            bool isBeginTxId,
            long beginTimestamp,
            bool isEndTxId,
            long endTimestamp,
            object recordKey,
            long versionKey,
            object record)
        {
            this.isBeginTxId = isBeginTxId;
            this.beginTimestamp = beginTimestamp;
            this.isEndTxId = isEndTxId;
            this.endTimestamp = endTimestamp;
            this.record = record;

            this.recordKey = recordKey;
            this.versionKey = versionKey;
        }

        // The special constructor is used to deserialize values.
        public VersionEntry(SerializationInfo info, StreamingContext context)
        {
            this.isBeginTxId = (bool) info.GetValue("isBeginTxId", typeof(bool));
            this.beginTimestamp = (long) info.GetValue("beginTimestamp", typeof(long));
            this.isEndTxId = (bool) info.GetValue("isEndTxId", typeof(bool));
            this.endTimestamp = (long) info.GetValue("endTimestamp", typeof(long));
            this.record = info.GetValue("record", typeof(object));

            this.recordKey = info.GetValue("recordKey", typeof(object));
            this.versionKey = (long) info.GetValue("versionKey", typeof(long));
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
            return this.versionKey == other.VersionKey &&
                   this.recordKey == other.recordKey &&
                   this.isBeginTxId == other.IsBeginTxId &&
                   this.beginTimestamp == other.BeginTimestamp &&
                   this.IsEndTxId == other.IsEndTxId &&
                   this.endTimestamp == other.EndTimestamp;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("isBeginTxId", this.IsBeginTxId, typeof(bool));
            info.AddValue("beginTimestamp", this.BeginTimestamp, typeof(long));
            info.AddValue("isEndTxId", this.IsEndTxId, typeof(bool));
            info.AddValue("endTimestamp", this.EndTimestamp, typeof(long));
            info.AddValue("record", this.Record, typeof(object));

            info.AddValue("recordKey", this.RecordKey, typeof(object));
            info.AddValue("versionKey", this.VersionKey, typeof(long));
        }
    }
}
