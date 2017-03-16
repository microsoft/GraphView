using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinEOp: GremlinTranslationOperator
    {
        public List<object> EdgeIdsOrElements { get; set; }

        public GremlinEOp(params object[] edgeIdsOrElements)
        {
            EdgeIdsOrElements = new List<object>(edgeIdsOrElements);
        }

        public GremlinEOp(List<object> edgeIdsOrElements)
        {
            EdgeIdsOrElements = edgeIdsOrElements;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable != null)
            {
                throw new QueryCompilationException("This step only can be a start step.");
            }
            GremlinFreeVertexVariable newVariable = new GremlinFreeVertexVariable();

            inputContext.VariableList.Add(newVariable);
            inputContext.TableReferences.Add(newVariable);
            inputContext.SetPivotVariable(newVariable);

            inputContext.PivotVariable.OutE(inputContext, new List<string>());

            if (EdgeIdsOrElements.Count > 0)
            {
                List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                foreach (var id in EdgeIdsOrElements)
                {
                    if (GremlinUtil.IsNumber(id) || id is string)
                    {
                        WScalarExpression firstExpr = inputContext.PivotVariable.GetVariableProperty(GremlinKeyword.EdgeID).ToScalarExpression();
                        WScalarExpression secondExpr = SqlUtil.GetValueExpr(id);
                        WBooleanComparisonExpression booleanExpr = SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr);
                        booleanExprList.Add(booleanExpr);
                    }
                }
                inputContext.AddPredicate(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));
            }

            return inputContext;
        }

    }
}
