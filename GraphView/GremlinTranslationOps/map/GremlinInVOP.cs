using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinInVOP: GremlinTranslationOperator
    {
        public GremlinInVOP() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVertexVariable newVertexVar = new GremlinVertexVariable();

            GremlinUtil.CheckIsGremlinEdgeVariable(inputContext.LastVariable);

            // When the last of inputContext.RemainingVariableList is    
            // V <--[Edge a] -- 
            // Then we add a new inner Vertex and the same edge for getting vertex of the inV
            // V <--[Edge a] -- innerVertex --[Edge a] -->
            if ((inputContext.LastVariable as GremlinEdgeVariable).EdgeType == GremlinEdgeType.InEdge) {
                var CurrentEdge = inputContext.LastVariable;
                inputContext.AddGremlinVariable(new GremlinVertexVariable());
                inputContext.AddGremlinVariable(new GremlinEdgeVariable(CurrentEdge.VariableName, 
                                                                            GremlinEdgeType.OutEdge));
            }

            inputContext.AddGremlinVariable(newVertexVar);
            inputContext.SetDefaultProjection(newVertexVar);

            return inputContext;
        }
    }
}
