using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslation
{
    internal class GremlinBothOp: GremlinTranslationOperator
    {
        public List<string> EdgeLabels { get; set; }

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

            if (inputContext.NewVariableList.Contains(inputContext.CurrVariable))
            {
                GremlinVertexVariable newVertexVariable = new GremlinVertexVariable();
                GremlinEdgeVariable newEdgeVariable = new GremlinEdgeVariable(newVertexVariable, WEdgeType.BothEdge);
                inputContext.AddNewVariable(newVertexVariable);
                inputContext.AddNewVariable(newEdgeVariable);
                inputContext.AddPaths(inputContext.CurrVariable, newEdgeVariable, newVertexVariable);

                foreach (var edgeLabel in EdgeLabels)
                {
                    WScalarExpression key = GremlinUtil.GetColumnReferenceExpression(newEdgeVariable.VariableName,
                        "label");
                    WBooleanComparisonExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, edgeLabel);
                    inputContext.AddPredicate(booleanExpr);
                }

                inputContext.SetCurrVariable(newVertexVariable);
                inputContext.SetDefaultProjection(newVertexVariable);
            }
            else
            {
                List<object> PropertyKeys = new List<object>();
                inputContext.CurrVariable.Properties.Add("_edge");
                inputContext.CurrVariable.Properties.Add("_reversed_edge");
                PropertyKeys.Add(GremlinUtil.GetValueExpression(inputContext.CurrVariable.VariableName + "._edge"));
                PropertyKeys.Add(GremlinUtil.GetValueExpression(inputContext.CurrVariable.VariableName + "._reversed_edge"));
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("bothE", PropertyKeys);
                WUnqualifiedJoin tableReference = new WUnqualifiedJoin()
                {
                    FirstTableRef = null,
                    SecondTableRef = secondTableRef,
                    UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
                };
                GremlinTVFEdgeVariable newEdgeVariable = new GremlinTVFEdgeVariable(tableReference, WEdgeType.BothEdge);
                inputContext.AddNewVariable(newEdgeVariable);

                GremlinVertexVariable newVertexVariable = new GremlinVertexVariable();
                inputContext.AddNewVariable(newVertexVariable);

                inputContext.AddPaths(null, newEdgeVariable, newVertexVariable);
                inputContext.SetCurrVariable(newVertexVariable);
                inputContext.SetDefaultProjection(newVertexVariable);
            }
            return inputContext;
        }
    }
}
