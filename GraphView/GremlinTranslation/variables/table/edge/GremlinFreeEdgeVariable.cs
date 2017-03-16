using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinFreeEdgeVariable(WEdgeType edgeType)
        {
            EdgeType = edgeType;
        }
    }
}
