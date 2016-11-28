using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinBothEOp: GremlinTranslationOperator
    {
        public List<string> EdgeLabels;

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

            //GremlinVertexVariable asSource = new GremlinVertexVariable();
            //GremlinVertexVariable asTarget = new GremlinVertexVariable();
            //GremlinEdgeVariable outE = new GremlinEdgeVariable(GremlinEdgeType.OutEdge);
            //GremlinEdgeVariable inE = new GremlinEdgeVariable(GremlinEdgeType.InEdge);
            //inputContext.AddNewVariable(asSource);
            //inputContext.AddNewVariable(asTarget);
            //inputContext.AddPaths(inputContext.CurrVariable, outE, asTarget);
            //inputContext.AddPaths(asSource, inE, inputContext.CurrVariable);

            //GremlinJoinEdgeVariable newVariable = new GremlinJoinEdgeVariable(outE, inE);
            //inputContext.AddNewVariable(newVariable);
            //inputContext.SetDefaultProjection(newVariable);
            //inputContext.SetCurrVariable(newVariable);

            GremlinVertexVariable newVertexVar = new GremlinVertexVariable();
            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(WEdgeType.BothEdge);
            inputContext.AddNewVariable(newVertexVar, Labels);
            inputContext.AddNewVariable(newEdgeVar, Labels);
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
