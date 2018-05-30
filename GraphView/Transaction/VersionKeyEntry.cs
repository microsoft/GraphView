using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    class VersionKeyEntry : TxSetEntry
    {
        internal long VersionKey;

        public VersionKeyEntry()
        {

        }

        public VersionKeyEntry(string tableId, object recordKey, long versionKey) :
            base(tableId, recordKey)
        {
            this.VersionKey = versionKey;
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
