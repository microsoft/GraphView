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
            VariableName = this.contextEdge.VariableName;
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

        internal override void InV(GremlinToSqlContext currentContext)
        {
            contextEdge.Populate("_sink");
            GremlinBoundVertexVariable newVariable = new GremlinBoundVertexVariable(new GremlinVariableProperty(contextEdge, "_sink"));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation: always introduce a new vertex variable
            contextEdge.Populate("_sink");
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(contextEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.PivotVariable = outVertex;
        }
    }
}
