using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVOp: GremlinTranslationOperator
    {
        public List<object> VertexIdsOrElements { get; set; }
        public GremlinVOp(params object[] vertexIdsOrElements)
        {
            VertexIdsOrElements = new List<object>(vertexIdsOrElements);
        }

        public GremlinVOp(List<object> vertexIdsOrElements)
        {
            VertexIdsOrElements = vertexIdsOrElements;
        }
        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinFreeVertexVariable newVariable = new GremlinFreeVertexVariable();

            if (VertexIdsOrElements.Count > 0)
            {
                List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                foreach (var id in VertexIdsOrElements)
                {
                    if (id is int || id is string)
                    {
                        WScalarExpression firstExpr =
                            newVariable.GetVariableProperty(GremlinKeyword.NodeID).ToScalarExpression();
                        WScalarExpression secondExpr = SqlUtil.GetValueExpr(id);
                        WBooleanComparisonExpression booleanExpr = SqlUtil.GetEqualBooleanComparisonExpr(firstExpr,
                            secondExpr);
                        booleanExprList.Add(booleanExpr);
                    }
                    else
                    {
                        throw new ArgumentException();
                    }
                }
                inputContext.AddPredicate(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));
            }

            inputContext.VariableList.Add(newVariable);
            inputContext.TableReferences.Add(newVariable);
            inputContext.SetPivotVariable(newVariable);

            return inputContext;
        }
    }
}
