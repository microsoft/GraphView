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

            GremlinTableVariable newVariable;
            if (inputContext.PivotVariable == null
                || inputContext.PivotVariable is GremlinFreeVertexVariable
                || inputContext.PivotVariable is GremlinContextVariable)
            {
                newVariable = new GremlinFreeVertexVariable();
            }
            else
            {
                newVariable = new GremlinBoundVertexVariable();
            }

            if (VertexIdsOrElements.Count > 0)
            {
                // old 
                List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                foreach (var id in VertexIdsOrElements)
                {
                    if (GremlinUtil.IsNumber(id) || id is string)
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
                // old
                // new yj
                //List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                //WScalarExpression firstExpr0 =
                //            newVariable.GetVariableProperty(GremlinKeyword.NodeID).ToScalarExpression();
                //string partitionInClause = string.Join(", ", VertexIdsOrElements.Select(partitionKey => $"'{partitionKey}'"));
                //WScalarExpression secondExpr = SqlUtil.GetValueExpr(partitionInClause);
                //partitionInClause = firstExpr0 + " IN (" + partitionInClause + ")";
                //WScalarExpression inExpr = SqlUtil.GetValueExpr(partitionInClause);
                ////WBooleanExpression boolInExpr = 
                //Predicate p = new Predicate(PredicateType.within);
                //p.IsTag = true;

                //WScalarExpression trueExpr = SqlUtil.GetValueExpr("true");
                //WBooleanExpression booleanExpr = SqlUtil.GetEqualBooleanComparisonExpr(trueExpr, inExpr);

                //WBooleanExpression booleanExpr = new WBooleanComparisonExpression()
                //{
                //    ComparisonType = BooleanComparisonType.Equals,
                //    FirstExpr = SqlUtil.GetFunctionCall(partitionInClause, null),
                //    SecondExpr = SqlUtil.GetValueExpr(true)
                //};
                //foreach (var id in VertexIdsOrElements)
                //{
                //    if (GremlinUtil.IsNumber(id) || id is string)
                //    {
                //        WScalarExpression firstExpr =
                //            newVariable.GetVariableProperty(GremlinKeyword.NodeID).ToScalarExpression();
                //        WScalarExpression secondExpr = SqlUtil.GetValueExpr(id);
                //        WBooleanComparisonExpression booleanExpr = SqlUtil.GetEqualBooleanComparisonExpr(firstExpr,
                //            secondExpr);
                //        booleanExprList.Add(booleanExpr);
                //    }
                //    else
                //    {
                //        throw new ArgumentException();
                //    }
                //}
                //var tempResult = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
                //inputContext.AddPredicate(booleanExpr);
                // new yj
            }

            inputContext.VariableList.Add(newVariable);
            inputContext.TableReferences.Add(newVariable);
            inputContext.SetPivotVariable(newVariable);

            return inputContext;
        }
    }
}
