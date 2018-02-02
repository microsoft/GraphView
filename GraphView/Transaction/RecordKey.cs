using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    public class RecordKey : Tuple<string, object>
    {
        internal RecordKey(string tableId, object recordId)
            : base(tableId, recordId) { }

        internal string TableId
        {
            get
            {
                return this.Item1;
            }
        }

        internal object RecordId
        {
            get
            {
                return this.Item2;
            }
        }

        public override bool Equals(object obj)
        {
            RecordKey other = (RecordKey)obj;
            if (other == null)
            {
                return false;
            }

            return (this.Item1 == other.Item1) && (this.Item2 == other.Item2);
        }

        public override int GetHashCode()
        {
            return this.Item1.GetHashCode() ^ this.Item2.GetHashCode();
        }
    }
}
