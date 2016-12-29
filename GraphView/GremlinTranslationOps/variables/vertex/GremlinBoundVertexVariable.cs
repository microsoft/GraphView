using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    /// <summary>
    /// A free vertex variable is translated to a node table reference in 
    /// the FROM clause, whereas a bound vertex variable is translated into
    /// a table-valued function following a prior table-valued function producing vertex references. 
    /// </summary>
    internal class GremlinBoundVertexVariable : GremlinVertexVariable2
    {
        private GremlinVariableProperty vertexId;
        private List<string> projectedProperties;

        public GremlinBoundVertexVariable(GremlinVariableProperty vertexId)
        {
            VariableName = GenerateTableAlias();
            this.vertexId = vertexId;
            projectedProperties = new List<string>();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal override void Populate(string name)
        {
            if (!projectedProperties.Contains(name))
            {
                projectedProperties.Add(name);
            }
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            Populate("BothAdjacencyList");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(adjacencyList);
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
