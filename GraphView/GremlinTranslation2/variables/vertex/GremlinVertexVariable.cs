using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinVertexVariable : GremlinTableVariable
    {
        protected static int _count = 0;
        internal override string GenerateTableAlias()
        {
            return "N_" + _count++;
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.NodeID);
        }
    }
}
