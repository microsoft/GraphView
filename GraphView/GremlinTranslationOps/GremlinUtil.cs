using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        internal static WMultiPartIdentifier GetMultiPartIdentifier(params string[] parts)
        {
            return ConvertListToMultiPartIdentifier(parts);
        }

        internal static WMultiPartIdentifier ConvertListToMultiPartIdentifier(string[] parts)
        {
            var MultiIdentifierList = new List<Identifier>();
            foreach (var part in parts)
            {
                MultiIdentifierList.Add(new Identifier() { Value = part });
            }
            return new WMultiPartIdentifier() { Identifiers = MultiIdentifierList };
        }

        internal static void CheckIsGremlinVertexVariable(GremlinVariable GremlinVar)
        {
            if (GremlinVar.GetType() != typeof(GremlinVertexVariable))
            {
                throw new Exception("It's not a GremlinVertexVariable");
            }
        }

        internal static void CheckIsGremlinEdgeVariable(GremlinVariable GremlinVar)
        {
            if (GremlinVar.GetType() != typeof(GremlinEdgeVariable)) {
                throw new Exception("It's not a GremlinEdgeVariable");
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
            if (value.GetType() == typeof(string))
            {
                return new WValueExpression(value as string, true);
            }
            else
            {
                return new WValueExpression(value as string, false);
            }
        }

        internal static WBooleanComparisonExpression GetBooleanComparisonExpression(GremlinVariable gremlinVar,
                                                                                     string key, object value)
        {
            WMultiPartIdentifier MultiIdentifierValue = GetMultiPartIdentifier(gremlinVar.VariableName, key);
            WScalarExpression ValueExpression = GetValueExpression(value);

            return new WBooleanComparisonExpression()
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr = GetColumnReferenceExpression(gremlinVar.VariableName, key),
                        SecondExpr = ValueExpression
                    };
        }

        internal static WBooleanComparisonExpression GetBooleanComparisonExpression(GremlinVariable gremlinVar,
                                                                             string key, Predicate predicate)
        {
            WMultiPartIdentifier MultiIdentifierValue = GremlinUtil.GetMultiPartIdentifier(gremlinVar.VariableName, key);
            WScalarExpression ValueExpression = GremlinUtil.GetValueExpression(predicate.Value);

            return new WBooleanComparisonExpression()
                    {
                        ComparisonType = GremlinUtil.GetComparisonTypeFromPredicateType(predicate.PredicateType),
                        FirstExpr = GremlinUtil.GetColumnReferenceExpression(gremlinVar.VariableName, key),
                        SecondExpr = ValueExpression
                    };
        }

        internal static WBooleanBinaryExpression GetBooleanBinaryExpression(WBooleanExpression booleanExpr1, WBooleanExpression booleanExpr2)
        {
            return new WBooleanBinaryExpression()
            {
                BooleanExpressionType = BooleanBinaryExpressionType.And,
                FirstExpr = booleanExpr1,
                SecondExpr = booleanExpr2
            };
        }
    }
}
