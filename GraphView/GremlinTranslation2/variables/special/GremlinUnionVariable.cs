using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinUnionVariable: GremlinTableVariable
    {
        public List<GremlinToSqlContext> UnionContextList { get; set; }

        public GremlinUnionVariable(List<GremlinToSqlContext> unionContextList)
        {
            UnionContextList = unionContextList;
        }

        public override WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }
}
