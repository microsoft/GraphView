// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 

using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public static class TsqlFragmentToString
    {
        public static string WSchemaObjectName(WSchemaObjectName sobj)
        {
            var sb = new StringBuilder(128);
            var empty = true;

            if (sobj.ServerIdentifier != null)
            {
                sb.Append(sobj.ServerIdentifier.Value);
                empty = false;
            }

            if (sobj.DatabaseIdentifier != null)
            {
                if (!empty)
                {
                    sb.Append('.');
                }

                sb.Append(sobj.DatabaseIdentifier.Value);
                empty = false;
            }

            if (sobj.BaseIdentifier == null) return sb.ToString();
            if (!empty)
            {
                sb.Append('.');
            }

            sb.Append(sobj.BaseIdentifier.Value);

            return sb.ToString();
        }

        public static string WMultipartIdentifier(WMultiPartIdentifier multiIdent)
        {
            var sb = new StringBuilder(128);

            for (var i = 0; i < multiIdent.Identifiers.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append('.');
                }
                sb.Append(multiIdent.Identifiers[i].Value);
            }

            return sb.ToString();
        }

        public static string BinaryQueryExpressionType(BinaryQueryExpressionType etype)
        {
            switch (etype)
            {
                case GraphView.BinaryQueryExpressionType.Union:
                    return "UNION";
                case GraphView.BinaryQueryExpressionType.Intersect:
                    return "INTERSECT";
                case GraphView.BinaryQueryExpressionType.Except:
                    return "EXCEPT ";
                default:
                    throw new GraphViewException("Invalid type of binary query expression");
            }
        }

        public static string JoinType(QualifiedJoinType jType, JoinHint jHint)
        {
            string joinTypeStr;
            switch (jType)
            {
                case QualifiedJoinType.FullOuter:
                    joinTypeStr = "FULL OUTER ";
                    break;
                case QualifiedJoinType.Inner:
                    joinTypeStr = "INNER ";
                    break;
                case QualifiedJoinType.LeftOuter:
                    joinTypeStr = "LEFT OUTER ";
                    break;
                case QualifiedJoinType.RightOuter:
                    joinTypeStr = "RIGHT OUTER ";
                    break;
                default:
                    throw new GraphViewException("Invalid join type");
            }
            switch (jHint)
            {
                case JoinHint.Hash:
                    joinTypeStr += "HASH ";
                    break;
                case JoinHint.Loop:
                    joinTypeStr += "LOOP ";
                    break;
                case JoinHint.Merge:
                    joinTypeStr += "MERGE ";
                    break;
                default:
                    break;
            }
            joinTypeStr += "JOIN";
            return joinTypeStr;
        }

        public static string JoinType(UnqualifiedJoinType jType)
        {
            string joinTypeStr;
            switch (jType)
            {
                case UnqualifiedJoinType.CrossApply:
                    joinTypeStr = "CROSS APPLY ";
                    break;
                case UnqualifiedJoinType.CrossJoin:
                    joinTypeStr = "CROSS JOIN";
                    break;
                case UnqualifiedJoinType.OuterApply:
                    joinTypeStr = "OUTER APPLY";
                    break;
                default:
                    throw new GraphViewException("Invalid join type");
            }
            return joinTypeStr;
        }

        public static string BinaryExpressionType(BinaryExpressionType btype)
        {
            switch (btype)
            {
                case GraphView.BinaryExpressionType.Add:
                    return "+";
                case GraphView.BinaryExpressionType.BitwiseAnd:
                    return "&";
                case GraphView.BinaryExpressionType.BitwiseOr:
                    return "|";
                case GraphView.BinaryExpressionType.BitwiseXor:
                    return "^";
                case GraphView.BinaryExpressionType.Divide:
                    return "/";
                case GraphView.BinaryExpressionType.Modulo:
                    return "%";
                case GraphView.BinaryExpressionType.Multiply:
                    return "*";
                case GraphView.BinaryExpressionType.Subtract:
                    return "-";
                default:
                    throw new GraphViewException("Invalid binary expression type");
            }
        }

        public static string BooleanExpressionType(BooleanBinaryExpressionType btype)
        {
            switch (btype)
            {
                case BooleanBinaryExpressionType.And:
                    return "AND";
                case BooleanBinaryExpressionType.Or:
                    return "OR";
                default:
                    throw new GraphViewException("Invalid boolean expression type");
            }
        }

        public static string BooleanComparisonType(BooleanComparisonType ctype)
        {
            switch (ctype)
            {
                case GraphView.BooleanComparisonType.Equals:
                    return "=";
                case GraphView.BooleanComparisonType.GreaterThan:
                    return ">";
                case GraphView.BooleanComparisonType.GreaterThanOrEqualTo:
                    return ">=";
                case GraphView.BooleanComparisonType.LessThan:
                    return "<";
                case GraphView.BooleanComparisonType.LessThanOrEqualTo:
                    return "<=";
                case GraphView.BooleanComparisonType.NotEqualToBrackets:
                    return "<>";
                case GraphView.BooleanComparisonType.NotEqualToExclamation:
                    return "!=";
                case GraphView.BooleanComparisonType.NotGreaterThan:
                    return "!>";
                case GraphView.BooleanComparisonType.NotLessThan:
                    return "!<";
                default:
                    throw new GraphViewException("Invalid boolean expression type");
            }
        }

        public static string SortOrder(SortOrder order)
        {
            switch (order)
            {
                case GraphView.SortOrder.Ascending:
                    return "ASC";
                case GraphView.SortOrder.Descending:
                    return "DESC";
                case GraphView.SortOrder.NotSpecified:
                    return "";
                default:
                    throw new GraphViewException("Invalid sort order");
            }
        }

        public static string DataType(WDataTypeReference dtype)
        {
            switch (dtype.GetType().Name)
            {
                case "SqlDataTypeReference":
                    {
                        var sqltype = dtype as WSqlDataTypeReference;
                        var sb = new StringBuilder(1024);
                        sb.Append(WSchemaObjectName(sqltype.Name));
                        if (sqltype.Parameters.Any())
                        {
                            sb.Append("(");
                            for (var j = 0; j < sqltype.Parameters.Count; ++j)
                            {
                                if (j > 0)
                                    sb.Append(", ");
                                sb.Append(sqltype.Parameters[j].Value);
                            }
                            sb.Append(")");
                        }
                        return sb.ToString();
                    }
                default:
                    return "";
            }
        }

        public static string AssignmentType(AssignmentKind kind)
        {
            switch (kind)
            {
                case AssignmentKind.AddEquals:
                    return "+=";
                case AssignmentKind.BitwiseAndEquals:
                    return "&=";
                case AssignmentKind.BitwiseOrEquals:
                    return "|=";
                case AssignmentKind.BitwiseXorEquals:
                    return "^=";
                case AssignmentKind.DivideEquals:
                    return "/=";
                case AssignmentKind.Equals:
                    return "=";
                case AssignmentKind.ModEquals:
                    return "%=";
                case AssignmentKind.MultiplyEquals:
                    return "*=";
                case AssignmentKind.SubtractEquals:
                    return "-=";
                default:
                    throw new GraphViewException("Invalid assignment kind.");
            }
        }

        public static string UnaryExpressionType(UnaryExpressionType type)
        {
            switch (type)
            {
                case GraphView.UnaryExpressionType.Positive:
                    return "+";
                case GraphView.UnaryExpressionType.Negative:
                    return "-";
                case GraphView.UnaryExpressionType.BitwiseNot:
                    return "~";
                default:
                    throw new GraphViewException("Invalid unary expression type.");
            }
        }

        public static string OptimizerHintKind(OptimizerHintKind type)
        {
            switch (type)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.Unspecified:
                    return "";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.HashGroup:
                    return "Hash Group";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.OrderGroup:
                    return "Order Group";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.MergeJoin:
                    return "Merge Join";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.HashJoin:
                    return "Hash Join";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.LoopJoin:
                    return "Loop Join";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.ConcatUnion:
                    return "Concat Union";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.HashUnion:
                    return "Hash Union";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.MergeUnion:
                    return "Merge Union";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.KeepUnion:
                    return "Keep Union";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.ForceOrder:
                    return "Force Order";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.RobustPlan:
                    return "Robust Plan";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.KeepPlan:
                    return "Keep Plan";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.KeepFixedPlan:
                    return "KeepFixed Plan";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.ExpandViews:
                    return "Expand Views";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.AlterColumnPlan:
                    return "AlterColumnPlan";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.ShrinkDBPlan:
                    return "ShrinkDBPlan";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.BypassOptimizerQueue:
                    return "BypassOptimizerQueue";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.UsePlan:
                    return "Use Plan";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.ParameterizationSimple:
                    return "Parameterization Simple";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.ParameterizationForced:
                    return "Parameterization Forced";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.OptimizeCorrelatedUnionAll:
                    return "OptimizeCorrelatedUnionAll";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.Recompile:
                    return "Recompile";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.Fast:
                    return "Fast";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.CheckConstraintsPlan:
                    return "CheckConstraintsPlan";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.MaxRecursion:
                    return "MaxRecursion";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.MaxDop:
                    return "MaxDop";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.QueryTraceOn:
                    return "QueryTraceOn";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.CardinalityTunerLimit:
                    return "CardinalityTunerLimit";
                //case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.TableHints:
                //    return "TableHints";
                //case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.OptimizeFor:
                //    return "OptimizeFor";
                case Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHintKind.IgnoreNonClusteredColumnStoreIndex:
                    return "Ignore_NonClustered_ColumnStore_Index";
                default:
                    throw new GraphViewException("Invalid optimize hint kind.");
            }
        }
        
    }
}
