using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextEdgeVariable : GremlinEdgeVariable2
    {
        GremlinEdgeVariable2 contextEdge;

        public GremlinContextEdgeVariable(GremlinEdgeVariable2 contextEdge)
        {
            this.contextEdge = contextEdge;
        }

        public override GremlinVariableType GetVariableType()
        {
            return contextEdge.GetVariableType();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return contextEdge.DefaultProjection();
        }

        internal override void Populate(string name)
        {
            contextEdge.Populate(name);
        }

        internal override void OutV(GremlinToSqlContext2 currentContext)
        {
            // A naive implementation: always introduce a new vertex variable
            contextEdge.Populate("_sink");
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(contextEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(outVertex);
        }
    }
}
