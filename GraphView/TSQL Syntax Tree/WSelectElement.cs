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
using System.Globalization;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public enum AssignmentKind
    {
        AddEquals,
        BitwiseAndEquals,
        BitwiseOrEquals,
        BitwiseXorEquals,
        DivideEquals,
        Equals,
        ModEquals,
        MultiplyEquals,
        SubtractEquals
    }

    /// <summary>
    /// Three types of elements in the SELECT clause:
    /// 1) scalar expressions.
    /// 2) star expressions.
    /// 3) local/global variables
    /// </summary>
    public abstract partial class WSelectElement : WSqlFragment { }

    public partial class WSelectScalarExpression : WSelectElement
    {
        internal WScalarExpression SelectExpr { get; set; }
        internal string ColumnName { get; set; }

        internal override bool OneLine()
        {
            return SelectExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            var line = SelectExpr.ToString(indent);

            return ColumnName != null ? string.Format(CultureInfo.CurrentCulture, "{0} AS {1}", line, ColumnName) : line;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (SelectExpr != null)
                SelectExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WSelectStarExpression : WSelectElement
    {
        internal WMultiPartIdentifier Qulifier { get; set; }
        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            if (Qulifier==null)
                return "*";
            else
            {
                return Qulifier.ToString() + "." + "*";
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }

    public partial class WSelectSetVariable : WSelectElement
    {
        internal WScalarExpression Expression { get; set; }
        internal string VariableName { get; set; }
        internal AssignmentKind AssignmentType { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (OneLine())
            {
                return string.Format(CultureInfo.InvariantCulture, "{0}{1} {2} {3}", indent, VariableName, 
                    TsqlFragmentToString.AssignmentType(AssignmentType), 
                    Expression.ToString(""));
            }
            return string.Format(CultureInfo.InvariantCulture, "{0}{1} {2}\r\n", indent, VariableName, 
                TsqlFragmentToString.AssignmentType(AssignmentType)) + Expression.ToString(indent + " ");
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

}
