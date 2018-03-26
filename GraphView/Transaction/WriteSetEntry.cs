using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    internal class WriteSetEntry
    {
        internal object RecordKey { get; set; }
        internal long VersionKey { get; set; }

        public WriteSetEntry(object recordKey, long versionKey)
        {
            this.RecordKey = recordKey;
            this.VersionKey = versionKey;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 23 + this.RecordKey.GetHashCode();
            hash = hash * 23 + this.VersionKey.GetHashCode();

            return hash;
        }

        public override bool Equals(object obj)
        {
            ReadSetEntry entry = obj as ReadSetEntry;
            if (entry == null)
            {
                return false;
            }

            return this.RecordKey == entry.RecordKey && this.VersionKey == entry.VersionKey;
        }
    }
}
