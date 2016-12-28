using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinBothEOp: GremlinTranslationOperator
    {
        public List<string> EdgeLabels { get; set; }

        public GremlinBothEOp(params string[] edgelabels)
        {
            EdgeLabels = new List<string>();
            foreach (var edgeLabel in edgelabels)
            {
                EdgeLabels.Add(edgeLabel);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinVertexVariable newVertexVar = new GremlinVertexVariable();
            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(newVertexVar,  WEdgeType.BothEdge);
            inputContext.AddNewVariable(newVertexVar);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddPaths(inputContext.CurrVariable, newEdgeVar, newVertexVar);

            foreach (var edgeLabel in EdgeLabels)
            {
                WScalarExpression key = GremlinUtil.GetColumnReferenceExpression(newEdgeVar.VariableName, "label");
                WBooleanComparisonExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, edgeLabel);
                inputContext.AddPredicate(booleanExpr);
            }

            inputContext.SetCurrVariable(newEdgeVar);
            inputContext.SetDefaultProjection(newEdgeVar);

            return inputContext;
        }
    }
}
