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

        public string GenerateTableAlias()
        {
            return "R_" + _count++;
        }
    }
}
