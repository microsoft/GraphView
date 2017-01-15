using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinListVariable: GremlinVariable
    {
        public List<GremlinVariable> GremlinVariableList;

        public GremlinListVariable(List<GremlinVariable> gremlinVariableList)
        {
            GremlinVariableList = new List<GremlinVariable>(gremlinVariableList);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(this, "listVar");
        }
    }
}
