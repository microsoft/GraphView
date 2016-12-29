using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeVertexVariable : GremlinVertexVariable2
    {
        public GremlinFreeVertexVariable()
        {
            VariableName = GenerateTableAlias();
        }

        internal override void Both(GremlinToSqlContext2 currentContext)
        {
            GremlinEdgeVariable2 bothEdgeVar = new GremlinBoundEdgeVariable(new GremlinVariableProperty(this, "BothAdjacencyList"));
            currentContext.VariableList.Add(bothEdgeVar);
            GremlinFreeVertexVariable bothVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(bothVertex);

            // In this case, the both-edge variable is not added to the table-reference list. 
            // Instead, we populate a path this_variable-[bothEdge]->bothVertex in the context
            currentContext.TableReferences.Add(bothVertex);

            currentContext.PivotVariable = bothVertex;
        }
    }
}
