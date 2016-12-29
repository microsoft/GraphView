using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVirtualVertexVariable : GremlinVariable
    {
        public GremlinEdgeVariable FromEdge { set; get; }

        public GremlinVirtualVertexVariable(GremlinEdgeVariable fromEdge)
        {
            FromEdge = fromEdge;
            VariableName = fromEdge.VariableName;
        }
    }
}
