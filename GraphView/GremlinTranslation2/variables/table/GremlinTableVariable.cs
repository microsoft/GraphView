using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable2, ISqlTable
    {
        protected static int _count = 0;

        internal virtual string GenerateTableAlias()
        {
            return "R_" + _count++;
        }

        public virtual List<WSelectElement> ToSelectElementList()
        {
            return null;
        }

        public virtual WTableReference ToTableReference()
        {
            return null;
        }
    }
}
