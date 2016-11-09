using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinOutEOp: GremlinTranslationOperator
    {
        internal List<string> EdgeLabels;

        public GremlinOutEOp(params string[] labels)
        {
            EdgeLabels = new List<string>();
            foreach (var label in labels)
            {
                EdgeLabels.Add(label);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);

            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(GremlinEdgeType.OutEdge);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.SetDefaultProjection(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            GremlinVertexVariable sinkVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(sinkVar);

            inputContext.AddPaths(inputContext.CurrVariable, newEdgeVar, sinkVar);

            inputContext.SetCurrentVariable(newEdgeVar);

            return inputContext;
        }
    }
}
