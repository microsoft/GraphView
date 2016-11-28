using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinInEOp: GremlinTranslationOperator
    {
        internal List<string> EdgeLabels;

        public GremlinInEOp(params string[] labels)
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

            //GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);

            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(WEdgeType.InEdge);
            inputContext.AddNewVariable(newEdgeVar, Labels);
            inputContext.SetDefaultProjection(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            GremlinVertexVariable sourceVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(sourceVar, Labels);

            inputContext.AddPaths(sourceVar, newEdgeVar, inputContext.CurrVariable);

            inputContext.SetCurrVariable(newEdgeVar);

            return inputContext;          
        }
    }
}
