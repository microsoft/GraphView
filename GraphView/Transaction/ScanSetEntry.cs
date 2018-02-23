using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    internal class ScanSetEntry
    {
        internal object Key { get; }
        internal long ReadTimestamp { get; }
        internal bool HasVisibleVersion { get; }

        public ScanSetEntry(object key, long readTimestamp, bool hasVisibleVersion)
        {
            this.Key = key;
            this.ReadTimestamp = readTimestamp;
            this.HasVisibleVersion = hasVisibleVersion;
        }

        public override int GetHashCode()
        {
            return this.Key.GetHashCode() ^ this.ReadTimestamp.GetHashCode()
                ^ HasVisibleVersion.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            ScanSetEntry entry = obj as ScanSetEntry;
            if (entry == null)
            {
                return false;
            }

            return this.Key == entry.Key && this.ReadTimestamp == entry.ReadTimestamp
                && this.HasVisibleVersion == entry.HasVisibleVersion;
        }
    }
}
