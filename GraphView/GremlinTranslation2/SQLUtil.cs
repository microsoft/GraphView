using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class SqlUtil
    {
        internal static WColumnReferenceExpression GetColumnReferenceExpr(params string[] parts)
        {
            return new WColumnReferenceExpression() { MultiPartIdentifier = GetMultiPartIdentifier(parts) };
        }

        internal static WColumnReferenceExpression GetStarColumnReferenceExpr()
        {
            return new WColumnReferenceExpression() { ColumnType = ColumnType.Wildcard };
        }

        internal static WMultiPartIdentifier GetMultiPartIdentifier(params string[] parts)
        {
            var multiIdentifierList = new List<Identifier>();
            foreach (var part in parts)
            {
                multiIdentifierList.Add(GetIdentifier(part));
            }
            return new WMultiPartIdentifier() { Identifiers = multiIdentifierList };
        }

        internal static Identifier GetIdentifier(string value)
        {
            return new Identifier() { Value = value };
        }

        internal static BooleanComparisonType GetComparisonType(PredicateType predicateType)
        {
            if (predicateType == PredicateType.eq) return BooleanComparisonType.Equals;
            if (predicateType == PredicateType.neq) return BooleanComparisonType.NotEqualToExclamation;
            if (predicateType == PredicateType.lt) return BooleanComparisonType.LessThan;
            if (predicateType == PredicateType.lte) return BooleanComparisonType.LessThanOrEqualTo;
            if (predicateType == PredicateType.gt) return BooleanComparisonType.GreaterThan;
            if (predicateType == PredicateType.gte) return BooleanComparisonType.GreaterThanOrEqualTo;
            throw new Exception("Error: GetComparisonTypeFromPredicateType");
        }

        internal static WValueExpression GetValueExpr(object value)
        {
            if (value == null) return new WValueExpression("null", false);
            return !(value is string) ? new WValueExpression(value.ToString(), false)
                                      : new WValueExpression(value.ToString(), true);
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

        internal static WBooleanExpression GetBooleanComparisonExpr(WScalarExpression firstExpr, WScalarExpression secondExpr, Predicate predicate)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();     
            switch (predicate.PredicateType)
            {
                case PredicateType.within:
                    foreach (var value in predicate.Values)
                    {
                        secondExpr = GetValueExpr(value);
                        booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, secondExpr,
                            GetComparisonType(PredicateType.eq)));
                    }
                    return GetBooleanParenthesisExpr(ConcatBooleanExprWithOr(booleanExprList));
                case PredicateType.without:
                    foreach (var value in predicate.Values)
                    {
                        secondExpr = GetValueExpr(value);
                        booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, secondExpr,
                            GetComparisonType(PredicateType.neq)));
                    }
                    return GetBooleanParenthesisExpr(ConcatBooleanExprWithAnd(booleanExprList));
                case PredicateType.inside:
                    throw new NotImplementedException();
                case PredicateType.outside:
                    throw new NotImplementedException();
                case PredicateType.between:
                    throw new NotImplementedException();
                default:
                    return GetBooleanComparisonExpr(firstExpr, secondExpr, GetComparisonType(predicate.PredicateType));
            }
        }

        internal static WBooleanComparisonExpression GetEqualBooleanComparisonExpr(WScalarExpression firstExpr, WScalarExpression secondExpr)
        {
            return GetBooleanComparisonExpr(firstExpr, secondExpr, BooleanComparisonType.Equals);
        }

        internal static WBooleanBinaryExpression GetBooleanBinaryExpr(WBooleanExpression firstExpr,
            WBooleanExpression secondExpr, BooleanBinaryExpressionType type)
        {
            return new WBooleanBinaryExpression()
            {
                BooleanExpressionType = type,
                FirstExpr = firstExpr,
                SecondExpr = secondExpr
            };
        }

        internal static WBooleanBinaryExpression GetAndBooleanBinaryExpr(WBooleanExpression firstExpr,
            WBooleanExpression secondExpr)
        {
            return GetBooleanBinaryExpr(firstExpr, secondExpr, BooleanBinaryExpressionType.And);
        }

        internal static WExistsPredicate GetExistPredicate(WSelectQueryBlock subQueryExpr)
        {
            return new WExistsPredicate() { Subquery = GetScalarSubquery(subQueryExpr) };
        }

        internal static WBooleanNotExpression GetNotExistPredicate(WSelectQueryBlock subQueryExpr)
        {
            return new WBooleanNotExpression() { Expression = GetExistPredicate(subQueryExpr) };
        }

        internal static WBooleanExpression ConcatBooleanExprWithOr(List<WBooleanExpression> booleanExprList)
        {
            return ConcatBooleanExpressionList(booleanExprList, BooleanBinaryExpressionType.Or);
        }

        internal static WBooleanExpression ConcatBooleanExprWithAnd(List<WBooleanExpression> booleanExprList)
        {
            return ConcatBooleanExpressionList(booleanExprList, BooleanBinaryExpressionType.And);
        }

        internal static WBooleanExpression ConcatBooleanExpressionList(List<WBooleanExpression> booleanExprList,
            BooleanBinaryExpressionType type)
        {
            if (booleanExprList.Count == 1) return booleanExprList.First();
            WBooleanExpression concatExpr = null;
            foreach (var booleanExpr in booleanExprList)
            {
                concatExpr = concatExpr == null ? booleanExpr
                                                : GetBooleanBinaryExpr(booleanExpr, concatExpr, type);
            }
            return concatExpr;
        }

        internal static WSchemaObjectName GetSchemaObjectName(string value)
        {
            return new WSchemaObjectName(GetIdentifier(value));
        }

        internal static WNamedTableReference GetNamedTableReference(string value)
        {
            return new WNamedTableReference() { TableObjectName = GetSchemaObjectName(value) };
        }

        internal static WNamedTableReference GetNamedTableReference(GremlinVariable gremlinVar)
        {
            return new WNamedTableReference()
            {
                Alias = GetIdentifier(gremlinVar.VariableName),
                TableObjectString = "node",
                TableObjectName = GetSchemaObjectName("node"),
                Low = gremlinVar.Low,
                High = gremlinVar.High
            };
        }

        internal static WBooleanExpression GetHaskeyBooleanExpr(GremlinVariable currVar, string key)
        {
            var firstExpr = GetFunctionCall("has_key", GetColumnReferenceExpr(currVar.VariableName, key));
            var secondExpr = GetValueExpr("true");
            return GetEqualBooleanComparisonExpr(firstExpr, secondExpr);
        }

        internal static WFunctionCall GetFunctionCall(string functionName, params WScalarExpression[] parameterList)
        {
            return new WFunctionCall()
            {
                FunctionName = GetIdentifier(functionName),
                Parameters = new List<WScalarExpression>(parameterList)
            };
        }

        internal static WSelectScalarExpression GetSelectScalarExpr(WScalarExpression valueExpr, string alias = null)
        {
            return new WSelectScalarExpression()
            {
                SelectExpr = valueExpr,
                ColumnName = alias
            };
        }

        internal static WExpressionWithSortOrder GetExpressionWithSortOrder(string key, GremlinKeyword.Order order)
        {
            return new WExpressionWithSortOrder()
            {
                ScalarExpr = GetColumnReferenceExpr(key),
                SortOrder = ConvertGremlinOrderToSqlOrder(order)
            };
        }

        internal static SortOrder ConvertGremlinOrderToSqlOrder(GremlinKeyword.Order order)
        {
            if (GremlinKeyword.Order.Desr == order) return SortOrder.Descending;
            if (GremlinKeyword.Order.Incr == order) return SortOrder.Ascending;
            if (GremlinKeyword.Order.Shuffle == order) return SortOrder.NotSpecified;
            throw new NotImplementedException();
        }

        internal static WGroupingSpecification GetGroupingSpecification(string key)
        {
            return new WExpressionGroupingSpec() { Expression = GetColumnReferenceExpr(key) };
        }

        internal static WMatchPath GetMatchPath(GremlinMatchPath path)
        {
            return new WMatchPath()
            {
                PathEdgeList = GetPathEdgeList(path),
                Tail = path.SinkVariable == null ? null : GetSchemaObjectName(path.SinkVariable.VariableName)
            };
        }

        internal static List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>> GetPathEdgeList(GremlinMatchPath path)
        {
            return new List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>>()
            {
                new Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>(
                    path.SourceVariable == null ? null : GetSchemaObjectName(path.SourceVariable.VariableName),
                    GetEdgeColumnReferenceExpr(path.EdgeVariable as GremlinEdgeTableVariable)
                )
            };
        }

        internal static WEdgeColumnReferenceExpression GetEdgeColumnReferenceExpr(GremlinEdgeTableVariable edge)
        {
            return new WEdgeColumnReferenceExpression()
            {
                MultiPartIdentifier = GetMultiPartIdentifier("Edge"),
                Alias = edge.VariableName,
                MinLength = 1,
                MaxLength = 1,
                EdgeType = edge.EdgeType
            };
        }

        internal static WBooleanParenthesisExpression GetBooleanParenthesisExpr(WBooleanExpression booleanExpr)
        {
            return new WBooleanParenthesisExpression() { Expression = booleanExpr };
        }

        internal static WSchemaObjectFunctionTableReference GetFunctionTableReference(string functionName,
            List<WScalarExpression> parameterList, GremlinVariable gremlinvariable, string alias = null)
        {
            WSchemaObjectFunctionTableReference funcTableRef;
            switch (functionName)
            {
                case GremlinKeyword.func.Coalesce:
                    funcTableRef = new WCoalesceTableReference();
                    break;
                case GremlinKeyword.func.Constant:
                    funcTableRef = new WConstantReference();
                    break;
                case GremlinKeyword.func.OutE:
                    funcTableRef = new WBoundOutEdgeTableReference();
                    break;
                case GremlinKeyword.func.InE:
                    funcTableRef = new WBoundOutEdgeTableReference();
                    break;
                case GremlinKeyword.func.BothE:
                    funcTableRef = new WBoundBothEdgeTableReference();
                    break;
                case GremlinKeyword.func.BothForwardE:
                    funcTableRef = new WBoundBothForwardEdgeTableReference();
                    break;
                case GremlinKeyword.func.FlatMap:
                    funcTableRef = new WFlatMapTableReference();
                    break;
                case GremlinKeyword.func.Key:
                    funcTableRef = new WKeyTableReference();
                    break;
                case GremlinKeyword.func.Local:
                    funcTableRef = new WLocalTableReference();
                    break;
                case GremlinKeyword.func.OutV:
                    funcTableRef = new WBoundOutNodeTableReference();
                    break;
                case GremlinKeyword.func.InV:
                    funcTableRef = new WBoundOutNodeTableReference();
                    break;
                case GremlinKeyword.func.BothV:
                    funcTableRef = new WBoundBothNodeTableReference();
                    break;
                case GremlinKeyword.func.Optional:
                    funcTableRef = new WOptionalTableReference();
                    break;
                case GremlinKeyword.func.Properties:
                    funcTableRef = new WPropertiesTableReference();
                    break;
                case GremlinKeyword.func.Repeat:
                    funcTableRef = new WRepeatTableReference();
                    break;
                case GremlinKeyword.func.Value:
                    funcTableRef = new WValueTableReference();
                    break;
                case GremlinKeyword.func.Values:
                    funcTableRef = new WValuesTableReference();
                    break;
                case GremlinKeyword.func.Unfold:
                    funcTableRef = new WUnfoldTableReference();
                    break;
                case GremlinKeyword.func.Union:
                    funcTableRef = new WUnionTableReference();
                    break;
                case GremlinKeyword.func.Project:
                    funcTableRef = new WProjectTableReference();
                    break;
                case GremlinKeyword.func.AddV:
                    funcTableRef = new WAddVTableReference();
                    break;
                case GremlinKeyword.func.AddE:
                    funcTableRef = new WAddETableReference();
                    break;
                case GremlinKeyword.func.SideEffect:
                    funcTableRef = new WSideEffectTableReference();
                    break;
                case GremlinKeyword.func.Dedup:
                    funcTableRef = new WDedupTableReference();
                    break;
                case GremlinKeyword.func.DropNode:
                    funcTableRef = new WDropNodeTableReference();
                    break;
                case GremlinKeyword.func.DropEdge:
                    funcTableRef = new WDropEdgeTableReference();
                    break;
                case GremlinKeyword.func.DropProperties:
                    funcTableRef = new WDropPropertiesTableReference();
                    break;
                case GremlinKeyword.func.UpdateNodeProperties:
                    funcTableRef = new WUpdateNodePropertiesTableReference();
                    break;
                case GremlinKeyword.func.UpdateEdgeProperties:
                    funcTableRef = new WUpdateEdgePropertiesTableReference();
                    break;
                default:
                    throw new NotImplementedException();
            }
            funcTableRef.SchemaObject = GetSchemaObjectName(functionName);
            funcTableRef.Parameters = parameterList;
            funcTableRef.Alias = GetIdentifier(alias);
            funcTableRef.Low = gremlinvariable.Low;
            funcTableRef.High = gremlinvariable.High;
            return funcTableRef;
        }

        internal static WScalarSubquery GetScalarSubquery(WSelectQueryExpression selectQueryBlock)
        {
            return new WScalarSubquery() { SubQueryExpr = selectQueryBlock };
        }

        internal static WVariableReference GetVariableReference(string name)
        {
            return new WVariableReference() { Name = "@" + name };
        }

        internal static WUnqualifiedJoin GetCrossApplyTableReference(WTableReference firstTableRef, WTableReference secondTableRef)
        {
            return new WUnqualifiedJoin()
            {
                FirstTableRef = firstTableRef,
                SecondTableRef = secondTableRef,
                UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
            };
        }

        internal static WQueryDerivedTable GetDerivedTable(WSelectQueryBlock selectQueryBlock, string alias)
        {
            return new WQueryDerivedTable()
            {
                QueryExpr = selectQueryBlock,
                Alias = GetIdentifier(alias)
            };
        }

        internal static WBooleanExpression GetTrueBooleanComparisonExpr()
        {
            var firstExpr = GetValueExpr("1");
            var secondExpr = GetValueExpr("1");
            return GetEqualBooleanComparisonExpr(firstExpr, secondExpr);
        }

        internal static WBooleanExpression GetFalseBooleanComparisonExpr()
        {
            var firstExpr = GetValueExpr("1");
            var secondExpr = GetValueExpr("0");
            return GetEqualBooleanComparisonExpr(firstExpr, secondExpr);
        }

        internal static WSelectScalarExpression GetSelectFunctionCall(string functionName, params WScalarExpression[] parameters)
        {
            return new WSelectScalarExpression() { SelectExpr = GetFunctionCall(functionName, parameters) };
        }

        internal static WBinaryQueryExpression GetBinaryQueryExpr(WSelectQueryExpression firstQueryExpr,
            WSelectQueryExpression secondQueryExpr)
        {
            return new WBinaryQueryExpression()
            {
                FirstQueryExpr = firstQueryExpr,
                SecondQueryExpr = secondQueryExpr,
                All = true,
                BinaryQueryExprType = BinaryQueryExpressionType.Union,
            };
        }

        internal static WWhereClause GetWhereClause(WBooleanExpression predicate)
        {
            return new WWhereClause() {SearchCondition = predicate};
        }

        internal static WVariableTableReference GetVariableTableReference(string variableName)
        {
            return new WVariableTableReference()
            {
                Variable = GetVariableReference(variableName),
                Alias = GetIdentifier(variableName)
            };
        }

        internal static WSetVariableStatement GetSetVariableStatement(string variableName, WScalarExpression scalarExpr)
        {
            return new WSetVariableStatement()
            {
                Expression = scalarExpr,
                Variable = GetVariableReference(variableName)
            };
        }

        internal static WSelectQueryBlock GetSimpleSelectQueryBlock(string variableName, List<string> projectProperties)
        {
            var queryBlock = new WSelectQueryBlock();
            foreach (var property in projectProperties)
            {
                if (property == null)
                {
                    queryBlock.SelectElements.Add(GetSelectScalarExpr(GetValueExpr(null)));
                }
                else
                {
                    queryBlock.SelectElements.Add(GetSelectScalarExpr(GetColumnReferenceExpr(variableName, property)));
                }
            }
            return queryBlock;
        }

        internal static WSelectQueryBlock GetSimpleSelectQueryBlock(string value)
        {
            var queryBlock = new WSelectQueryBlock();
            queryBlock.SelectElements.Add(GetSelectScalarExpr(GetValueExpr(value)));
            return queryBlock;
        }
    }
}
