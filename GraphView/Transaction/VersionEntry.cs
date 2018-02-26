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

        public object RecordKey { get; private set; }

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
            this.RecordKey = recordKey;
            this.record = record;
        }

        // The special constructor is used to deserialize values.
        public VersionEntry(SerializationInfo info, StreamingContext context)
        {
            this.isBeginTxId = (bool) info.GetValue("isBeginTxId", typeof(bool));
            this.beginTimestamp = (long) info.GetValue("beginTimestamp", typeof(long));
            this.isEndTxId = (bool) info.GetValue("isEndTxId", typeof(bool));
            this.endTimestamp = (long) info.GetValue("endTimestamp", typeof(long));
            this.record = info.GetValue("record", typeof(object));
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + this.IsBeginTxId.GetHashCode();
            hash = hash * 23 + this.BeginTimestamp.GetHashCode();
            hash = hash * 23 + this.IsEndTxId.GetHashCode();
            hash = hash * 23 + this.EndTimestamp.GetHashCode();
            hash = hash * 23 + this.Record.GetHashCode();

            return hash;
        }

        public override bool Equals(object obj)
        {
            VersionEntry ventry = obj as VersionEntry;
            if (ventry == null)
            {
                return false;
            }

            return this.RecordKey == ventry.RecordKey &&
                this.BeginTimestamp == ventry.BeginTimestamp;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("isBeginTxId", this.IsBeginTxId, typeof(bool));
            info.AddValue("beginTimestamp", this.BeginTimestamp, typeof(long));
            info.AddValue("isEndTxId", this.IsEndTxId, typeof(bool));
            info.AddValue("endTimestamp", this.EndTimestamp, typeof(long));
            info.AddValue("record", this.Record, typeof(object));
        }
    }
}
