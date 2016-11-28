using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinBothOp: GremlinTranslationOperator
    {
        public List<string> EdgeLabels;

        public GremlinBothOp(params string[] edgeLabels)
        {
            EdgeLabels = new List<string>();
            foreach (var edgeLabel in edgeLabels)
            {
                EdgeLabels.Add(edgeLabel);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinVertexVariable newVertexVariable = new GremlinVertexVariable();
            GremlinEdgeVariable newEdgeVariable = new GremlinEdgeVariable(WEdgeType.BothEdge);
            inputContext.AddNewVariable(newVertexVariable, Labels);
            inputContext.AddNewVariable(newEdgeVariable, Labels);
            inputContext.AddPaths(inputContext.CurrVariable, newEdgeVariable, newVertexVariable);

            foreach (var edgeLabel in EdgeLabels)
            {
                WScalarExpression key = GremlinUtil.GetColumnReferenceExpression(newEdgeVariable.VariableName, "label");
                WBooleanComparisonExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, edgeLabel);
                inputContext.AddPredicate(booleanExpr);
            }
    
            inputContext.SetCurrVariable(newVertexVariable);
            inputContext.SetDefaultProjection(newVertexVariable);
            return inputContext;
        }
    }
}
