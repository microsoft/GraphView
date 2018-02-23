using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    internal class ReadSetEntry
    {
        internal object Key { get; }
        internal long BeginTimestamp { get; }

        public ReadSetEntry(object key, long beginTimestamp)
        {
            this.Key = key;
            this.BeginTimestamp = beginTimestamp;
        }

        public override int GetHashCode()
        {
            return this.Key.GetHashCode() ^ this.BeginTimestamp.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            ReadSetEntry entry = obj as ReadSetEntry;
            if (entry == null)
            {
                return false;
            }

            return this.Key == entry.Key && this.BeginTimestamp == entry.BeginTimestamp;
        }
    }
}
