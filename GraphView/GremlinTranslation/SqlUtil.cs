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
                FirstExpr = firstExpr.Copy(),
                SecondExpr = secondExpr.Copy()
            };
        }

        internal static WBooleanExpression GetBooleanComparisonExpr(WScalarExpression firstExpr, WScalarExpression secondExpr, Predicate predicate)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            WScalarExpression lowExpr = null;
            WScalarExpression highExpr = null;
            switch (predicate.PredicateType)
            {
                case PredicateType.and:
                    var andPredicate = predicate as AndPredicate;
                    foreach (var p in andPredicate.PredicateList)
                    {
                        WScalarExpression secExpr = GetValueExpr(p.Value);
                        booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, secExpr, p));
                    }
                    return ConcatBooleanExprWithAnd(booleanExprList);
                case PredicateType.or:
                    var orPredicate = predicate as OrPredicate;
                    foreach (var p in orPredicate.PredicateList)
                    {
                        WScalarExpression secExpr = GetValueExpr(p.Value);
                        booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, secExpr, p));
                    }
                    return ConcatBooleanExprWithOr(booleanExprList);
                case PredicateType.within:
                    if (predicate.IsTag)
                    {
                        List<WScalarExpression> parameters = new List<WScalarExpression>();
                        parameters.Add(firstExpr);
                        parameters.Add(secondExpr);
                        return GetFunctionBooleanExpression("WithInArray", parameters);
                    }
                    else
                    {
                        foreach (var value in predicate.Values)
                        {
                            secondExpr = GetValueExpr(value);
                            booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, secondExpr, GetComparisonType(PredicateType.eq)));
                        }
                        return ConcatBooleanExprWithOr(booleanExprList);
                    }
                case PredicateType.without:
                    if (predicate.IsTag)
                    {
                        List<WScalarExpression> parameters = new List<WScalarExpression>();
                        parameters.Add(firstExpr);
                        parameters.Add(secondExpr);
                        return GetFunctionBooleanExpression("WithOutArray", parameters);
                    }
                    else
                    {
                        foreach (var value in predicate.Values)
                        {
                            secondExpr = GetValueExpr(value);
                            booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, secondExpr,
                                GetComparisonType(PredicateType.neq)));
                        }
                        return ConcatBooleanExprWithAnd(booleanExprList);
                    }

                case PredicateType.inside:
                    lowExpr = GetValueExpr(predicate.Low);
                    highExpr = GetValueExpr(predicate.High);
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, lowExpr, GetComparisonType(PredicateType.gt)));
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, highExpr, GetComparisonType(PredicateType.lt))); ;
                    return ConcatBooleanExprWithAnd(booleanExprList);
                case PredicateType.outside:
                    lowExpr = GetValueExpr(predicate.Low);
                    highExpr = GetValueExpr(predicate.High);
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, lowExpr, GetComparisonType(PredicateType.lt)));
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, highExpr, GetComparisonType(PredicateType.gt))); ;
                    return ConcatBooleanExprWithOr(booleanExprList);
                case PredicateType.between:
                    lowExpr = GetValueExpr(predicate.Low);
                    highExpr = GetValueExpr(predicate.High);
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, lowExpr, GetComparisonType(PredicateType.gte)));
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, highExpr, GetComparisonType(PredicateType.lt))); ;
                    return ConcatBooleanExprWithAnd(booleanExprList);
                case PredicateType.lteOrgte:
                    lowExpr = GetValueExpr(predicate.Low);
                    highExpr = GetValueExpr(predicate.High);
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, lowExpr, GetComparisonType(PredicateType.lte)));
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, highExpr, GetComparisonType(PredicateType.gte))); ;
                    return ConcatBooleanExprWithOr(booleanExprList);
                case PredicateType.gteAndlte:
                    lowExpr = GetValueExpr(predicate.Low);
                    highExpr = GetValueExpr(predicate.High);
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, lowExpr, GetComparisonType(PredicateType.gte)));
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, highExpr, GetComparisonType(PredicateType.lte))); ;
                    return ConcatBooleanExprWithAnd(booleanExprList);
                case PredicateType.ltOrgte:
                    lowExpr = GetValueExpr(predicate.Low);
                    highExpr = GetValueExpr(predicate.High);
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, lowExpr, GetComparisonType(PredicateType.lt)));
                    booleanExprList.Add(GetBooleanComparisonExpr(firstExpr, highExpr, GetComparisonType(PredicateType.gte))); ;
                    return ConcatBooleanExprWithOr(booleanExprList);
                default:
                    return GetBooleanComparisonExpr(firstExpr, secondExpr, GetComparisonType(predicate.PredicateType));
            }
        }

        internal static WBooleanExpression GetFunctionBooleanExpression(string functionName, List<WScalarExpression> parameters)
        {
            WBooleanExpression booleanExpr = new WBooleanComparisonExpression()
            {
                ComparisonType = BooleanComparisonType.Equals,
                FirstExpr = GetFunctionCall(functionName, parameters),
                SecondExpr = GetValueExpr(true)
            };
            return booleanExpr;
        }

        internal static WFunctionCall GetFunctionCall(string functionName, List<WScalarExpression> parameters)
        {
            return new WFunctionCall()
            {
                FunctionName = GetIdentifier(functionName),
                Parameters = parameters
            };
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
                if (booleanExpr == null) continue;
                WBooleanExpression newExpr = type == BooleanBinaryExpressionType.Or
                    ? GetBooleanParenthesisExpr(booleanExpr)
                    : booleanExpr;
                concatExpr = concatExpr == null ? booleanExpr
                                                : GetBooleanBinaryExpr(newExpr, concatExpr, type);
            }
            if (concatExpr == null)
                return null;
            if (type == BooleanBinaryExpressionType.Or)
                return GetBooleanParenthesisExpr(concatExpr);
            return concatExpr;
        }

        internal static WSchemaObjectName GetSchemaObjectName(string value)
        {
            return new WSchemaObjectName(GetIdentifier(value));
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

        internal static WMatchPath GetMatchPath(GremlinMatchPath path)
        {
            return new WMatchPath()
            {
                PathEdgeList = GetPathEdgeList(path),
                Tail = GetPathTail(path)
            };
        }

        internal static WSchemaObjectName GetPathTail(GremlinMatchPath path)
        {
            var edge = path.EdgeVariable;
            if (edge.EdgeType == WEdgeType.InEdge)
                return path.SourceVariable == null ? null : GetSchemaObjectName(path.SourceVariable?.GetVariableName());
            else
                return path.SinkVariable == null ? null : GetSchemaObjectName(path.SinkVariable?.GetVariableName());
        }

        internal static WSchemaObjectName GetPathSource(GremlinMatchPath path)
        {
            var edge = path.EdgeVariable;
            if (edge.EdgeType == WEdgeType.InEdge)
                return GetSchemaObjectName(path.SinkVariable.GetVariableName());
            else
                return GetSchemaObjectName(path.SourceVariable?.GetVariableName());
        }

        internal static List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>> GetPathEdgeList(GremlinMatchPath path)
        {
            return new List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>>()
            {
                new Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>(
                    GetPathSource(path),
                    GetEdgeColumnReferenceExpr(path.EdgeVariable)
                )
            };
        }

        internal static WEdgeColumnReferenceExpression GetEdgeColumnReferenceExpr(GremlinFreeEdgeVariable edgeTable)
        {
            return new WEdgeColumnReferenceExpression()
            {
                MultiPartIdentifier = GetMultiPartIdentifier("Edge"),
                Alias = edgeTable.GetVariableName(),
                MinLength = 1,
                MaxLength = 1,
                EdgeType = edgeTable.EdgeType
            };
        }

        internal static WBooleanParenthesisExpression GetBooleanParenthesisExpr(WBooleanExpression booleanExpr)
        {
            return new WBooleanParenthesisExpression() { Expression = booleanExpr };
        }

        internal static WSchemaObjectFunctionTableReference GetFunctionTableReference(string functionName,
            List<WScalarExpression> parameterList, string alias = null)
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
                    funcTableRef = new WBoundInEdgeTableReference();
                    break;
                case GremlinKeyword.func.BothE:
                    funcTableRef = new WBoundBothEdgeTableReference();
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
                case GremlinKeyword.func.EtoV:
                    funcTableRef = new WBoundOutNodeTableReference();
                    break;
                case GremlinKeyword.func.V:
                    funcTableRef = new WBoundNodeTableReference();
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
                case GremlinKeyword.func.AllProperties:
                    funcTableRef = new WAllPropertiesTableReference();
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
                case GremlinKeyword.func.AllValues:
                    funcTableRef = new WAllValuesTableReference();
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
                    funcTableRef = new WAddVTableReference2();
                    break;
                case GremlinKeyword.func.AddE:
                    funcTableRef = new WAddETableReference();
                    break;
                case GremlinKeyword.func.SideEffect:
                    funcTableRef = new WSideEffectTableReference();
                    break;
                case GremlinKeyword.func.DedupGlobal:
                    funcTableRef = new WDedupGlobalTableReference();
                    break;
                case GremlinKeyword.func.DedupLocal:
                    funcTableRef = new WDedupLocalTableReference();
                    break;
                case GremlinKeyword.func.Drop:
                    funcTableRef = new WDropTableReference();
                    break;
                case GremlinKeyword.func.UpdateProperties:
                    funcTableRef = new WUpdatePropertiesTableReference();
                    break;
                case GremlinKeyword.func.Inject:
                    funcTableRef = new WInjectTableReference();
                    break;
                case GremlinKeyword.func.Path:
                    funcTableRef = new WPathTableReference();
                    break;
                case GremlinKeyword.func.Expand:
                    funcTableRef = new WExpandTableReference();
                    break;
                case GremlinKeyword.func.Map:
                    funcTableRef = new WMapTableReference();
                    break;
                case GremlinKeyword.func.Group:
                    funcTableRef = new WGroupTableReference();
                    break;
                case GremlinKeyword.func.Store:
                    funcTableRef = new WStoreTableReference();
                    break;
                case GremlinKeyword.func.Aggregate:
                    funcTableRef = new WAggregateTableReference();
                    break;
                case GremlinKeyword.func.Coin:
                    funcTableRef = new WCoinTableReference();
                    break;
                case GremlinKeyword.func.CountLocal:
                    funcTableRef = new WCountLocalTableReference();
                    break;
                case GremlinKeyword.func.MinLocal:
                    funcTableRef = new WMinLocalTableReference();
                    break;
                case GremlinKeyword.func.MaxLocal:
                    funcTableRef = new WMaxLocalTableReference();
                    break;
                case GremlinKeyword.func.MeanLocal:
                    funcTableRef = new WMeanLocalTableReference();
                    break;
                case GremlinKeyword.func.SumLocal:
                    funcTableRef = new WSumLocalTableReference();
                    break;
                case GremlinKeyword.func.OrderGlobal:
                    funcTableRef = new WOrderGlobalTableReference();
                    break;
                case GremlinKeyword.func.OrderLocal:
                    funcTableRef = new WOrderLocalTableReference();
                    break;
                case GremlinKeyword.func.Path2:
                    funcTableRef = new WPath2TableReference();
                    break;
                case GremlinKeyword.func.Range:
                    funcTableRef = new WRangeTableReference();
                    break;
                case GremlinKeyword.func.Decompose1:
                    funcTableRef = new WDecompose1TableReference();
                    break;
                case GremlinKeyword.func.Tree:
                    funcTableRef = new WTreeTableReference();
                    break;
                case GremlinKeyword.func.SimplePath:
                    funcTableRef = new WSimplePathTableReference();
                    break;
                case GremlinKeyword.func.CyclicPath:
                    funcTableRef = new WCyclicPathTableReference();
                    break;
                case GremlinKeyword.func.ValueMap:
                    funcTableRef = new WValueMapTableReference();
                    break;
                case GremlinKeyword.func.PropertyMap:
                    funcTableRef = new WPropertyMapTableReference();
                    break;
                case GremlinKeyword.func.SampleGlobal:
                    funcTableRef = new WSampleGlobalTableReference();
                    break;
                case GremlinKeyword.func.SampleLocal:
                    funcTableRef = new WSampleLocalTableReference();
                    break;
                case GremlinKeyword.func.Barrier:
                    funcTableRef = new WBarrierTableReference();
                    break;
                case GremlinKeyword.func.Choose:
                    funcTableRef = new WChooseTableReference();
                    break;
                case GremlinKeyword.func.ChooseWithOptions:
                    funcTableRef = new WChooseWithOptionsTableReference();
                    break;
                case GremlinKeyword.func.Select:
                    funcTableRef = new WSelectTableReference();
                    break;
                case GremlinKeyword.func.SelectOne:
                    funcTableRef = new WSelectOneTableReference();
                    break;
                case GremlinKeyword.func.SelectColumn:
                    funcTableRef = new WSelectColumnTableReference();
                    break;
                case GremlinKeyword.func.GraphViewId:
                    funcTableRef = new WIdTableReference();
                    break;
                case GremlinKeyword.func.GraphViewLabel:
                    funcTableRef = new WLabelTableReference();
                    break;
                default:
                    throw new NotImplementedException();
            }
            funcTableRef.SchemaObject = GetSchemaObjectName(functionName);
            funcTableRef.Parameters = parameterList;
            funcTableRef.Alias = GetIdentifier(alias);
            return funcTableRef;
        }

        internal static WScalarSubquery GetScalarSubquery(WSelectQueryExpression selectQueryBlock)
        {
            return new WScalarSubquery() { SubQueryExpr = selectQueryBlock };
        }

        internal static WUnqualifiedJoin GetCrossApplyTableReference(WTableReference secondTableRef)
        {
            return new WUnqualifiedJoin()
            {
                FirstTableRef = null,
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

        internal static WSelectQueryBlock GetSimpleSelectQueryBlock(string value)
        {
            var queryBlock = new WSelectQueryBlock();
            queryBlock.SelectElements.Add(GetSelectScalarExpr(GetValueExpr(value)));
            return queryBlock;
        }
    }
}
