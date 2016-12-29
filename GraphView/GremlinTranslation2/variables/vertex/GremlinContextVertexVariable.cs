using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVertexVariable : GremlinVertexVariable2
    {
        GremlinVertexVariable2 contextVariable;

        public GremlinContextVertexVariable(GremlinVertexVariable2 contextVariable)
        {
            this.contextVariable = contextVariable;
        }

        public override GremlinVariableType GetVariableType()
        {
            return contextVariable.GetVariableType();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return contextVariable.DefaultProjection();
        }

        internal override void Populate(string name)
        {
            contextVariable.Populate(name);
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            Populate("BothAdjacencyList");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            GremlinEdgeVariable2 bothEdge = new GremlinBoundEdgeVariable(adjacencyList);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);

            currentContext.TableReferences.Add(bothEdge);
            currentContext.TableReferences.Add(bothVertex);

            currentContext.PivotVariable = bothVertex;
        }
    }
}
