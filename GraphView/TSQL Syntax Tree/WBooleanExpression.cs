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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public enum BooleanBinaryExpressionType
    {
        And,
        Or
    }

    public enum BooleanComparisonType
    {
        Equals,
        GreaterThan,
        GreaterThanOrEqualTo,
        LeftOuterJoin,
        LessThan,
        LessThanOrEqualTo,
        NotEqualToBrackets,
        NotEqualToExclamation,
        NotGreaterThan,
        NotLessThan,
        RightOuterJoin
    }

    public enum SubqueryComparisonPredicateType
    {
        All,
        Any,
        None
    }

    public abstract partial class WBooleanExpression : WSqlFragment { }

    public partial class WBooleanBinaryExpression : WBooleanExpression
    {
        internal BooleanBinaryExpressionType BooleanExpressionType { get; set; }
        internal WBooleanExpression FirstExpr { get; set; }
        internal WBooleanExpression SecondExpr { get; set; }

        internal override bool OneLine()
        {
            return FirstExpr.OneLine() && SecondExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.Append(FirstExpr.ToString(indent));

            sb.AppendFormat(" {0} ", TsqlFragmentToString.BooleanExpressionType(BooleanExpressionType));

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(SecondExpr.ToString(indent));
            }

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder(512);

            sb.Append(FirstExpr.ToString(indent, useSquareBracket));

            sb.AppendFormat(" {0} ", TsqlFragmentToString.BooleanExpressionType(BooleanExpressionType));

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString("", useSquareBracket));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(SecondExpr.ToString(indent, useSquareBracket));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FirstExpr != null)
                FirstExpr.Accept(visitor);
            if (SecondExpr != null)
                SecondExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }

        /// <summary>
        /// Conjuncts two boolean expressions
        /// </summary>
        /// <param name="joinCondition">The first boolean expression</param>
        /// <param name="newCondition">The second boolean expression</param>
        /// <returns>The conjunctive boolean expression</returns>
        public static WBooleanExpression Conjunction(WBooleanExpression joinCondition, WBooleanExpression newCondition)
        {
            if (joinCondition == null)
            {
                return newCondition;
            }
            if (newCondition == null)
            {
                return joinCondition;
            }
            return new WBooleanBinaryExpression
            {
                FirstExpr = joinCondition,
                SecondExpr = newCondition,
                BooleanExpressionType = BooleanBinaryExpressionType.And
            };
        }
    }

    public partial class WBooleanComparisonExpression : WBooleanExpression
    {
        internal BooleanComparisonType ComparisonType { get; set; }
        internal WScalarExpression FirstExpr { get; set; }
        internal WScalarExpression SecondExpr { get; set; }

        internal override bool OneLine()
        {
            return FirstExpr.OneLine() && SecondExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.Append(FirstExpr.ToString(indent));

            sb.AppendFormat(" {0} ", TsqlFragmentToString.BooleanComparisonType(ComparisonType));

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(SecondExpr.ToString(indent + "    "));
            }

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder(512);

            sb.Append(FirstExpr.ToString(indent, useSquareBracket));

            sb.AppendFormat(" {0} ", TsqlFragmentToString.BooleanComparisonType(ComparisonType));

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString("", useSquareBracket));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(SecondExpr.ToString(indent + "    ", useSquareBracket));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FirstExpr != null)
                FirstExpr.Accept(visitor);
            if (SecondExpr != null)
                SecondExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WEdgeVertexBridgeExpression : WBooleanComparisonExpression { }

    public partial class WBooleanIsNullExpression : WBooleanExpression
    {
        internal bool IsNot { get; set; }
        internal WScalarExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            return string.Format(CultureInfo.CurrentCulture, IsNot ? "{0} IS NOT NULL" : "{0} IS NULL", Expression.ToString(indent));
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            return string.Format(CultureInfo.CurrentCulture, IsNot ? "{0} IS NOT NULL" : "{0} IS NULL", Expression.ToString(indent, useSquareBracket));
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Expression != null)
                Expression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WBooleanNotExpression : WBooleanExpression
    {
        internal WBooleanExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (Expression.OneLine())
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}NOT {1}", indent, Expression.ToString(""));
            }
            else
            {
                var line1 = string.Format(CultureInfo.CurrentCulture, "{0}NOT\r\n", indent);
                return line1 + Expression.ToString(indent);
            }
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            if (Expression.OneLine())
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}NOT {1}", indent, Expression.ToString("", useSquareBracket));
            }
            else
            {
                var line1 = string.Format(CultureInfo.CurrentCulture, "{0}NOT\r\n", indent);
                return line1 + Expression.ToString(indent, useSquareBracket);
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Expression != null)
                Expression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WBooleanParenthesisExpression : WBooleanExpression
    {
        internal WBooleanExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (Expression.OneLine())
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}({1})", indent, Expression.ToString(""));
            }
            var sb = new StringBuilder(512);

            sb.AppendFormat("{0}(\r\n", indent);
            sb.AppendFormat("{0}\r\n", Expression.ToString(indent));
            sb.AppendFormat("{0})", indent);

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            if (Expression.OneLine())
            {
                return string.Format(CultureInfo.CurrentCulture, "{0}({1})", indent, Expression.ToString("", useSquareBracket));
            }
            var sb = new StringBuilder(512);

            sb.AppendFormat("{0}(\r\n", indent);
            sb.AppendFormat("{0}\r\n", Expression.ToString(indent, useSquareBracket));
            sb.AppendFormat("{0})", indent);

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Expression != null)
                Expression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WBetweenExpression : WBooleanExpression
    {
        internal bool NotDefined { get; set; }
        internal WScalarExpression FirstExpr { get; set; }
        internal WScalarExpression SecondExpr { get; set; }
        internal WScalarExpression ThirdExpr { get; set; }

        internal override bool OneLine()
        {
            return FirstExpr.OneLine()
                && SecondExpr.OneLine()
                && ThirdExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(128);

            sb.Append(FirstExpr.ToString(indent));

            sb.Append(NotDefined ? " NOT BETWEEN " : " BETWEEN ");

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(SecondExpr.ToString(indent + "    "));
            }

            sb.Append(" AND ");

            if (ThirdExpr.OneLine())
            {
                sb.Append(ThirdExpr.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(ThirdExpr.ToString(indent));
            }

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder(128);

            sb.Append(FirstExpr.ToString(indent, useSquareBracket));

            sb.Append(NotDefined ? " NOT BETWEEN " : " BETWEEN ");

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString("", useSquareBracket));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(SecondExpr.ToString(indent + "    ", useSquareBracket));
            }

            sb.Append(" AND ");

            if (ThirdExpr.OneLine())
            {
                sb.Append(ThirdExpr.ToString("", useSquareBracket));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(ThirdExpr.ToString(indent, useSquareBracket));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FirstExpr != null)
                FirstExpr.Accept(visitor);
            if (SecondExpr != null)
                SecondExpr.Accept(visitor);
            if (ThirdExpr != null)
                ThirdExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WExistsPredicate : WBooleanExpression
    {
        internal WScalarSubquery Subquery { get; set; }

        internal override bool OneLine()
        {
            return Subquery.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            if (OneLine())
            {
                sb.AppendFormat("{0}EXISTS {1}", indent, Subquery.ToString(""));
            }
            else
            {
                sb.AppendFormat("{0}EXISTS (\r\n", indent);
                sb.AppendFormat(Subquery.SubQueryExpr.ToString(indent + "  "));
                sb.AppendFormat("\r\n{0})", indent);
            }

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder(512);

            if (OneLine())
            {
                sb.AppendFormat("{0}EXISTS {1}", indent, Subquery.ToString("", useSquareBracket));
            }
            else
            {
                sb.AppendFormat("{0}EXISTS (\r\n", indent);
                sb.AppendFormat(Subquery.SubQueryExpr.ToString(indent + "  ", useSquareBracket));
                sb.AppendFormat("\r\n{0})", indent);
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Subquery != null)
                Subquery.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WInPredicate : WBooleanExpression
    {
        internal WScalarExpression Expression { get; set; }
        internal bool NotDefined { get; set; }
        internal WScalarSubquery Subquery { get; set; }
        internal IList<WScalarExpression> Values { get; set; }

        internal WInPredicate()
        {
        }

        internal WInPredicate(WScalarExpression expression, List<string> values)
        {
            this.Expression = expression;
            this.Values = new List<WScalarExpression>();
            this.NotDefined = false;
            foreach (string value in values)
            {
                this.Values.Add(new WValueExpression(value, true));
            }
        }

        internal override bool OneLine()
        {
            var oneLineValue = Values != null && Values.All(e => e.OneLine());
            return Expression.OneLine() && Subquery == null && oneLineValue;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.AppendFormat(CultureInfo.CurrentCulture, Expression.ToString(indent));

            sb.Append(NotDefined ? " NOT IN " : " IN ");

            if (Values != null)
            {
                var newLine = false;

                for (var i = 0; i < Values.Count; i++)
                {
                    sb.Append(i > 0 ? ", " : "(");

                    if (Values[i].OneLine())
                    {
                        sb.Append(Values[i].ToString(""));
                    }
                    else
                    {
                        sb.Append("\r\n");
                        sb.Append(Values[i].ToString(indent + " "));
                        newLine = true;
                    }
                }

                if (newLine)
                {
                    sb.Append("\r\n");
                    sb.AppendFormat("{0})", indent);
                }
                else
                {
                    sb.Append(")");
                }
            }
            else if (Subquery != null)
            {
                sb.Append("\r\n");
                sb.Append(Subquery.ToString(indent + " "));
            }

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder(512);

            sb.AppendFormat(CultureInfo.CurrentCulture, Expression.ToString(indent, useSquareBracket));

            sb.Append(NotDefined ? " NOT IN " : " IN ");

            if (Values != null)
            {
                var newLine = false;

                for (var i = 0; i < Values.Count; i++)
                {
                    sb.Append(i > 0 ? ", " : "(");

                    if (Values[i].OneLine())
                    {
                        sb.Append(Values[i].ToString("", useSquareBracket));
                    }
                    else
                    {
                        sb.Append("\r\n");
                        sb.Append(Values[i].ToString(indent + " ", useSquareBracket));
                        newLine = true;
                    }
                }

                if (newLine)
                {
                    sb.Append("\r\n");
                    sb.AppendFormat("{0})", indent);
                }
                else
                {
                    sb.Append(")");
                }
            }
            else if (Subquery != null)
            {
                sb.Append("\r\n");
                sb.Append(Subquery.ToString(indent + " ", useSquareBracket));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Expression != null)
                Expression.Accept(visitor);
            if (Subquery != null)
                Subquery.Accept(visitor);
            if (Values != null)
            {
                var index = 0;
                for (var count = Values.Count; index < count; ++index)
                    Values[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WLikePredicate : WBooleanExpression
    {
        internal WScalarExpression EscapeExpr { get; set; }
        internal WScalarExpression FirstExpr { get; set; }
        internal WScalarExpression SecondExpr { get; set; }
        internal bool NotDefined { get; set; }

        internal override bool OneLine()
        {
            return FirstExpr.OneLine() && SecondExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.AppendFormat(CultureInfo.CurrentCulture, FirstExpr.ToString(indent));

            sb.Append(NotDefined ? " NOT LIKE " : " LIKE ");

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.AppendFormat(CultureInfo.CurrentCulture, SecondExpr.ToString(indent + " "));
            }

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder(512);

            sb.AppendFormat(CultureInfo.CurrentCulture, FirstExpr.ToString(indent, useSquareBracket));

            sb.Append(NotDefined ? " NOT LIKE " : " LIKE ");

            if (SecondExpr.OneLine())
            {
                sb.Append(SecondExpr.ToString("", useSquareBracket));
            }
            else
            {
                sb.Append("\r\n");
                sb.AppendFormat(CultureInfo.CurrentCulture, SecondExpr.ToString(indent + " ", useSquareBracket));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (EscapeExpr != null)
                EscapeExpr.Accept(visitor);
            if (FirstExpr != null)
                FirstExpr.Accept(visitor);
            if (SecondExpr != null)
                SecondExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WSubqueryComparisonPredicate : WBooleanExpression
    {
        internal BooleanComparisonType ComparisonType { get; set; }
        internal WScalarExpression Expression { get; set; }
        internal WScalarSubquery Subquery { get; set; }
        internal SubqueryComparisonPredicateType SubqueryComparisonType { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();

            sb.Append(Expression.ToString(indent));
            sb.AppendFormat(" {0} \r\n", TsqlFragmentToString.BooleanComparisonType(ComparisonType));
            sb.Append(Subquery.ToString(indent + " "));

            return sb.ToString();
        }

        internal override string ToString(string indent, bool useSquareBracket)
        {
            var sb = new StringBuilder();

            sb.Append(Expression.ToString(indent, useSquareBracket));
            sb.AppendFormat(" {0} \r\n", TsqlFragmentToString.BooleanComparisonType(ComparisonType));
            sb.Append(Subquery.ToString(indent + " ", useSquareBracket));

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Expression != null)
                Expression.Accept(visitor);
            if (Subquery != null)
                Subquery.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }
}
