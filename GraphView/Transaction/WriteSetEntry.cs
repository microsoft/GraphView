using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    class WriteSetEntry : TxSetEntry
    {
        internal object Payload { get; set; }

        public WriteSetEntry()
        {

        }

        public WriteSetEntry(object payload)
        {
            this.Payload = payload;
        }

        public WriteSetEntry(string tableId, object recordKey, object payload)
            : base(tableId, recordKey)
        {
            this.Payload = payload;
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
