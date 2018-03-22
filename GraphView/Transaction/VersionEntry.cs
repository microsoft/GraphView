namespace GraphView.Transaction
{
    using Newtonsoft.Json.Linq;
    using System.Runtime.Serialization;
    using System;

    [Serializable]
    internal class VersionEntry : ISerializable
    {
        private readonly object recordKey;
        private readonly long versionKey;
        private long beginTimestamp;
        private long endTimestamp;
        private readonly object record;
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

        public object Record
        {
            get
            {
                return this.record;
            }
        }

        public long MaxCommitTs
        {
            get
            {
                return this.maxCommitTs;
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

        }
           

        // The special constructor is used to deserialize values.
        public VersionEntry(SerializationInfo info, StreamingContext context)
        {
         
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
            return false;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
           
        }
    }
}
