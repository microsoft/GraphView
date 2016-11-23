using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GremlinTranslationOps.map;

namespace GraphView.GremlinTranslationOps
{
    public class GremlinUtil
    {
        internal static WColumnReferenceExpression GetColumnReferenceExpression(params string[] parts)
        {
            return new WColumnReferenceExpression()
            {
                MultiPartIdentifier = ConvertListToMultiPartIdentifier(parts)
            };
        }

        internal static WColumnReferenceExpression GetStarColumnReferenceExpression()
        {
            return new WColumnReferenceExpression()
            {
                ColumnType = ColumnType.Wildcard,
            };
        }

        internal static WMultiPartIdentifier GetMultiPartIdentifier(params string[] parts)
        {
            return ConvertListToMultiPartIdentifier(parts);
        }

        internal static Identifier GetIdentifier(string value)
        {
            return new Identifier() {Value = value};
        }

        internal static WMultiPartIdentifier ConvertListToMultiPartIdentifier(string[] parts)
        {
            var multiIdentifierList = new List<Identifier>();
            foreach (var part in parts)
            {
                multiIdentifierList.Add(new Identifier() {Value = part});
            }
            return new WMultiPartIdentifier() {Identifiers = multiIdentifierList};
        }

        internal static void CheckIsGremlinVertexVariable(GremlinVariable gremlinVar)
        {
            if (gremlinVar.GetType() != typeof(GremlinVertexVariable))
            {
                throw new Exception("It's not a GremlinVertexVariable");
            }
        }

        internal static void CheckIsGremlinEdgeVariable(GremlinVariable gremlinVar)
        {
            if (gremlinVar.GetType() != typeof(GremlinEdgeVariable))
            {
                throw new Exception("It's not a GremlinEdgeVariable");
            }
        }

        internal static void CheckIsGremlinAddEVariable(GremlinVariable gremlinVar)
        {
            if (gremlinVar.GetType() != typeof(GremlinAddEVariable))
            {
                throw new Exception("It's not a GremlinAddEVariable");
            }
        }

        internal static BooleanComparisonType GetComparisonTypeFromPredicateType(PredicateType predicateType)
        {
            if (predicateType == PredicateType.eq) return BooleanComparisonType.Equals;
            if (predicateType == PredicateType.neq) return BooleanComparisonType.NotEqualToExclamation;
            if (predicateType == PredicateType.lt) return BooleanComparisonType.LessThan;
            if (predicateType == PredicateType.lte) return BooleanComparisonType.LessThanOrEqualTo;
            if (predicateType == PredicateType.gt) return BooleanComparisonType.GreaterThan;
            if (predicateType == PredicateType.gte) return BooleanComparisonType.GreaterThanOrEqualTo;
            throw new Exception("Error: GetComparisonTypeFromPredicateType");
        }

        internal static WValueExpression GetValueExpression(object value)
        {
            if (value is string)
            {
                return new WValueExpression(value as string, true);
            }
            else
            {
                return new WValueExpression(value.ToString(), false);
            }
        }

        internal static WBooleanComparisonExpression GetBooleanComparisonExpr(GremlinVariable gremlinVar,
            string key, object value)
        {
            WScalarExpression valueExpression = GetValueExpression(value);

            return new WBooleanComparisonExpression()
            {
                ComparisonType = BooleanComparisonType.Equals,
                FirstExpr = GetColumnReferenceExpression(gremlinVar.VariableName, key),
                SecondExpr = valueExpression
            };
        }

        internal static WBooleanComparisonExpression GetBooleanComparisonExpr(WScalarExpression firstExpr,
            WScalarExpression secondExpr, BooleanComparisonType type)
        {
            return new WBooleanComparisonExpression()
            {
                ComparisonType = type,
                FirstExpr = firstExpr,
                SecondExpr = secondExpr
            };
        }

        internal static WBooleanExpression GetBooleanComparisonExpr(GremlinVariable gremlinVar,
            string key, Predicate predicate)
        {
            if (predicate.PredicateType == PredicateType.within ||
                predicate.PredicateType == PredicateType.without ||
                predicate.PredicateType == PredicateType.inside ||
                predicate.PredicateType == PredicateType.outside ||
                predicate.PredicateType == PredicateType.between)
            {
                List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                switch (predicate.PredicateType)
                {
                    case PredicateType.within:
                        foreach (var value in predicate.Values)
                        {
                            booleanExprList.Add(GetBooleanComparisonExpr(gremlinVar, key,
                                new Predicate(PredicateType.eq, value, predicate.IsAliasValue)));
                        }
                        return ConcatBooleanExpressionListWithOr(booleanExprList);
                    case PredicateType.without:
                        foreach (var value in predicate.Values)
                        {
                            booleanExprList.Add(GetBooleanComparisonExpr(gremlinVar, key,
                                new Predicate(PredicateType.neq, value, predicate.IsAliasValue)));
                        }
                        return ConcatBooleanExpressionListWithAnd(booleanExprList);
                    case PredicateType.inside:
                        //TODO
                        return null;
                    case PredicateType.outside:
                        //TODO
                        return null;
                    case PredicateType.between:
                        //TODO
                        return null;
                    default:
                        return null;
                }
            }
            else
            {
                WScalarExpression valueExpression = null;
                if (predicate.IsAliasValue)
                {
                    valueExpression = GetColumnReferenceExpression(predicate.Value as string, "id");
                }
                else
                {
                    valueExpression = GetValueExpression(predicate.Value);
                }
                return new WBooleanComparisonExpression()
                {
                    ComparisonType = GetComparisonTypeFromPredicateType(predicate.PredicateType),
                    FirstExpr = GetColumnReferenceExpression(gremlinVar.VariableName, key),
                    SecondExpr = valueExpression
                };
            }
        }

        internal static WBooleanBinaryExpression GetAndBooleanBinaryExpr(WBooleanExpression booleanExpr1,
            WBooleanExpression booleanExpr2)
        {
            return new WBooleanBinaryExpression()
            {
                BooleanExpressionType = BooleanBinaryExpressionType.And,
                FirstExpr = booleanExpr1,
                SecondExpr = booleanExpr2
            };
        }

        internal static WExistsPredicate GetExistPredicate(WSqlStatement subQueryExpr)
        {
            return new WExistsPredicate()
            {
                Subquery = new WScalarSubquery
                {
                    SubQueryExpr = subQueryExpr as WSelectQueryExpression
                }
        };
        }

        internal static WBooleanExpression ConcatBooleanExpressionListWithOr(List<WBooleanExpression> booleanExprList)
        {
            return ConcatBooleanExpressionList(booleanExprList, BooleanBinaryExpressionType.Or);
        }

        internal static WBooleanExpression ConcatBooleanExpressionListWithAnd(List<WBooleanExpression> booleanExprList)
        {
            return ConcatBooleanExpressionList(booleanExprList, BooleanBinaryExpressionType.And);
        }

        internal static WBooleanExpression ConcatBooleanExpressionList(List<WBooleanExpression> booleanExprList,
            BooleanBinaryExpressionType type)
        {
            WBooleanExpression concatExpr = null;
            foreach (var booleanExpr in booleanExprList)
            {
                if (booleanExpr != null && concatExpr != null)
                    concatExpr = new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = type,
                        FirstExpr = booleanExpr,
                        SecondExpr = concatExpr
                    };
                if (booleanExpr != null && concatExpr == null)
                    concatExpr = booleanExpr;
            }
            return concatExpr;
        }

        internal static WSchemaObjectName GetSchemaObjectName(string value)
        {
            return new WSchemaObjectName()
            {
                Identifiers = new List<Identifier>() {new Identifier() {Value = value}}
            };
        }

        internal static WNamedTableReference GetNamedTableReference(string value)
        {
            return new WNamedTableReference()
            {
                TableObjectName = GetSchemaObjectName(value)
            };
        }

        internal static WNamedTableReference GetNamedTableReference(GremlinVariable gremlinVar)
        {
            return new WNamedTableReference()
            {
                Alias = new Identifier() { Value = gremlinVar.VariableName },
                TableObjectString = "node",
                TableObjectName = new WSchemaObjectName(new Identifier() { Value = "node" }),
                Low = gremlinVar.Low,
                High = gremlinVar.High
            };
        }

        internal static WBooleanExpression GetHasKeyBooleanExpression(GremlinVariable currVar, string key)
        {
            WFunctionCall functionCall = new WFunctionCall()
            {
                FunctionName = GremlinUtil.GetIdentifier("IS_DEFINED"),
                Parameters = new List<WScalarExpression>()
                {
                    new WColumnReferenceExpression()
                    {
                        MultiPartIdentifier = GetMultiPartIdentifier(currVar.VariableName, key)
                    }
                }
            };
            WBooleanExpression booleanExpr = new WBooleanComparisonExpression()
            {
                ComparisonType = BooleanComparisonType.Equals,
                FirstExpr = functionCall,
                SecondExpr =
                    new WColumnReferenceExpression() {MultiPartIdentifier = GetMultiPartIdentifier("true")}
            };
            return booleanExpr;
        }

        internal static WFunctionCall GetFunctionCall(string functionName, params WScalarExpression[] parameters)
        {
            IList<WScalarExpression> parameterList = new List<WScalarExpression>();
            foreach (var parameter in parameters)
            {
                parameterList.Add(parameter);
            }
            return new WFunctionCall()
            {
                FunctionName = GetIdentifier(functionName),
                Parameters = parameters
            };
        }

        internal static WSelectScalarExpression GetSelectScalarExpression(WScalarExpression valueExpr)
        {
            return new WSelectScalarExpression() {SelectExpr = valueExpr};
        }

        internal static WExpressionWithSortOrder GetExpressionWithSortOrder(string key, Order order)
        {
            return new WExpressionWithSortOrder()
            {
                ScalarExpr = GetColumnReferenceExpression(key),
                SortOrder = ConvertGremlinOrderToSqlOrder(order)
            };
        }

        internal static SortOrder ConvertGremlinOrderToSqlOrder(Order order)
        {
            if (Order.Desr == order) return SortOrder.Descending;
            if (Order.Incr == order) return SortOrder.Ascending;
            if (Order.Shuffle == order) return SortOrder.NotSpecified;
            return SortOrder.Descending;
        }

        internal static WGroupingSpecification GetGroupingSpecification(string key)
        {
            return new WExpressionGroupingSpec()
            {
                Expression = GetColumnReferenceExpression(key)
            };
        }

        internal static Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression> GetPathExpression(
            Tuple<GremlinVariable, GremlinVariable, GremlinVariable> path)
        {
            WEdgeType edgeType = GetEdgeType(path.Item2);

            return new Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>(
                GetSchemaObjectName(path.Item1.VariableName),
                new WEdgeColumnReferenceExpression()
                {
                    MultiPartIdentifier = new WMultiPartIdentifier()
                    {
                        Identifiers = new List<Identifier>() {new Identifier() {Value = "Edge"}}
                    },
                    Alias = path.Item2.VariableName,
                    MinLength = 1,
                    MaxLength = 1,
                    EdgeType =  edgeType
                }
            );
        }

        internal static WEdgeType GetEdgeType(GremlinVariable edgeVar)
        {
            if ((edgeVar as GremlinEdgeVariable).EdgeType == GremlinEdgeType.BothEdge)
                return WEdgeType.BothEdge;
            if ((edgeVar as GremlinEdgeVariable).EdgeType == GremlinEdgeType.InEdge)
                return WEdgeType.InEdge;
            if ((edgeVar as GremlinEdgeVariable).EdgeType == GremlinEdgeType.OutEdge)
                return WEdgeType.OutEdge;

            return WEdgeType.OutEdge;
        }

        internal static WBooleanParenthesisExpression GetBooleanParenthesisExpression(WBooleanExpression booleanExpr)
        {
            return new WBooleanParenthesisExpression()
            {
                Expression = booleanExpr
            };
        }

        internal static WUnqualifiedJoin GetUnqualifiedJoin(GremlinVariable currVar, GremlinVariable lastVar)
        {
            var joinVertexVar = currVar as GremlinJoinVertexVariable;
            WSchemaObjectFunctionTableReference secondTableRef = new WSchemaObjectFunctionTableReference()
            {
                Alias = GremlinUtil.GetIdentifier(currVar.VariableName),
                Parameters = new List<WScalarExpression>()
                {
                    GetColumnReferenceExpression(joinVertexVar.LeftVariable.VariableName),
                    GetColumnReferenceExpression(joinVertexVar.RightVariable.VariableName )
                },
                SchemaObject = new WSchemaObjectName()
                {
                    Identifiers = new List<Identifier>() { GremlinUtil.GetIdentifier("BothV") }
                }
            };

            return new WUnqualifiedJoin()
            {
                FirstTableRef = GetNamedTableReference(lastVar),
                SecondTableRef = secondTableRef,
                UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
            };
        }

        internal static GremlinToSqlContext ProcessByFunctionStep(string functionName, GremlinToSqlContext inputContext, List<string> labels)
        {
            if (inputContext.Projection.Count > 1) throw new Exception("Can't process more than two projection");

            Projection projection = inputContext.Projection.First().Item2;
            WScalarExpression parameter = null;
            if (projection is ColumnProjection)
            {
                string projectionValue = (projection as ColumnProjection).Value;
                parameter = GetColumnReferenceExpression(inputContext.CurrVariable.VariableName, projectionValue);
            }
            else
            {
                //projection is ConstantProjection || projection is StarProjection
                parameter = GetStarColumnReferenceExpression();
            }

            inputContext.SetCurrProjection(GetFunctionCall(functionName, parameter));

            GremlinToSqlContext newContext = new GremlinToSqlContext();
            GremlinScalarVariable newScalarVariable = new GremlinScalarVariable(inputContext.ToSqlQuery());
            newContext.AddNewVariable(newScalarVariable, labels);
            newContext.SetCurrVariable(newScalarVariable);
            newContext.SetStarProjection(newScalarVariable);

            return newContext;
        }

        internal static void InheritedVariableFromParent(GremlinTranslationOperator op, GremlinToSqlContext inputContext)
        {
            while (op.InputOperator != null)
            {
                op = op.InputOperator;
            }
            if (op.GetType() == typeof(GremlinParentContextOp))
            {
                GremlinParentContextOp rootAsContextOp = op as GremlinParentContextOp;
                rootAsContextOp.InheritedVariable = inputContext.CurrVariable;
            }
        }

        internal static void InheritedContextFromParent(GraphTraversal2 traversal, GremlinToSqlContext inputContext)
        {
            GremlinTranslationOperator rootOp = traversal.GetEndOp();
            while (rootOp.InputOperator != null)
            {
                rootOp = rootOp.InputOperator;
            }
            if (rootOp.GetType() == typeof(GremlinParentContextOp))
            {
                GremlinParentContextOp rootAsContextOp = rootOp as GremlinParentContextOp;
                rootAsContextOp.SetContext(inputContext);
            }
        }

        //internal static void GetTrue
    }
}
