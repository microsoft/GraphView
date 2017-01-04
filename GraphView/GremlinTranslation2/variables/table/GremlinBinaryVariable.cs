using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    /// <summary>
    /// The abstract variable for coalesce and union 
    /// </summary>
    internal abstract class GremlinBinaryVariable : GremlinTableVariable
    {
        protected GremlinToSqlContext traversal1;
        protected GremlinToSqlContext traversal2;

        public GremlinBinaryVariable(GremlinToSqlContext traversal1, GremlinToSqlContext traversal2)
        {
            this.traversal1 = traversal1;
            this.traversal2 = traversal2;
        }

        internal override void Populate(string property)
        {
            traversal1.Populate(property);
            traversal2.Populate(property);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            //if (traversal1.PivotVariable.GetVariableType() != GremlinVariableType.Vertex &&
            //    traversal2.PivotVariable.GetVariableType() != GremlinVariableType.Vertex)
            //{
            //    // If neither output of the coalesce variable is of type vertex, 
            //    // this coalesce variable cannot be followed by the Both() step.
            //    base.Both(currentContext, edgeLabels);
            //}
            //else
            //{
            //    if (traversal1.PivotVariable.GetVariableType() == GremlinVariableType.Vertex)
            //    {
            //        traversal1.Populate("BothAdjacencyList");
            //    }

            //    if (traversal2.PivotVariable.GetVariableType() == GremlinVariableType.Vertex)
            //    {
            //        traversal2.Populate("BothAdjacencyList");
            //    }

            //    // Since a Gremlin binary variable is translated to a table-valued function in SQL,
            //    // the following both() is not described in the MATH caluse and the corresponding 
            //    // vertex variable is not a free vertex variable, but a bound vertex variable. 
            //    GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            //    GremlinEdgeVariable2 bothEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            //    bothEdge.Populate("_sink");
            //    currentContext.VariableList.Add(bothEdge);

            //    GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            //    currentContext.VariableList.Add(bothVertex);

            //    currentContext.TableReferences.Add(bothEdge);
            //    currentContext.TableReferences.Add(bothVertex);

            //    currentContext.PivotVariable = bothVertex;
            //}
        }
    }
}
