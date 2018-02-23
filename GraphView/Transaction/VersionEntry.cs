namespace GraphView.Transaction
{
    using Newtonsoft.Json.Linq;

    internal class VersionEntry
    {
        private bool isBeginTxId;
        private long beginTimestamp;
        private bool isEndTxId;
        private long endTimestamp;
        private object record;

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

        public JObject Record
        {
            get
            {
                return (JObject)this.record;
            }
            set
            {
                this.record = value;
            }
        }

        public VersionEntry(
            bool isBeginTxId, 
            long beginTimestamp, 
            bool isEndTxId, 
            long endTimestamp, 
            object recordKey, 
            JObject jObject)
        {
            this.isBeginTxId = isBeginTxId;
            this.beginTimestamp = beginTimestamp;
            this.isEndTxId = isEndTxId;
            this.endTimestamp = endTimestamp;
            this.RecordKey = recordKey;
            this.record = jObject;
        }

        public override bool Equals(object obj)
        {
            VersionEntry ventry = obj as VersionEntry;
            if (ventry == null)
            {
                return false;
            }

            return this.RecordKey == ventry.RecordKey && 
                this.IsBeginTxId == ventry.IsBeginTxId && 
                this.BeginTimestamp == ventry.BeginTimestamp;
        }
    }
}
