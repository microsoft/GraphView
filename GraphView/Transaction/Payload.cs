using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace GraphView.Transaction
{
    [Serializable]
    class Payload : ISerializable, ICloneable
    {
        internal object RecordKey { get; private set; }
        internal long VersionKey { get; private set; }
        internal long BeginTimestamp { get; private set; }
        internal long EndTimestamp { get; private set; }
        internal object Record { get; private set; }


        public Payload(
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            object record)
        {
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;            
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.Record = record;
        }

        public Payload(SerializationInfo info, StreamingContext context)
        {
            this.RecordKey = info.GetValue("recordKey", typeof(object));
            this.VersionKey = (long)info.GetValue("versionKey", typeof(long));
            this.BeginTimestamp = (long)info.GetValue("beginTimestamp", typeof(long));
            this.EndTimestamp = (long)info.GetValue("endTimestamp", typeof(long));
            this.Record = info.GetValue("record", typeof(long));
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
            Payload payload = obj as Payload;
            if (payload == null)
            {
                return false;
            }

            return this.VersionKey == payload.VersionKey &&
                this.RecordKey == payload.RecordKey;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("recordKey", this.RecordKey, typeof(object));
            info.AddValue("versionKey", this.VersionKey, typeof(long));
            info.AddValue("beginTimestamp", this.BeginTimestamp, typeof(long));
            info.AddValue("endTimestamp", this.EndTimestamp, typeof(long));
            info.AddValue("record", this.Record, typeof(object));
        }

        public object Clone()
        {
            return new Payload(this.RecordKey, this.VersionKey,
                this.BeginTimestamp, this.EndTimestamp, this.Record);
        }
    }
}
