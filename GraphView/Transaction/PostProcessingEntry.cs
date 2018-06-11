using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    class PostProcessingEntry : TxSetEntry
    {
        internal long VersionKey { get; set; }
        internal long BeginTimestamp { get; set; }
        internal long EndTimestamp { get; set; }

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
