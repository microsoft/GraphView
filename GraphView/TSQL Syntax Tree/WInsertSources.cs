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
using System.Linq;
using System.Text;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    public abstract partial class WInsertSource : WSqlFragment
    {
    }

    public partial class WSelectInsertSource : WInsertSource
    {
        public WSelectQueryExpression Select { get; set; }

        internal override bool OneLine()
        {
            return Select.OneLine();
        }

        internal override string ToString(string indent)
        {
            return Select.ToString(indent);
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Select != null)
                Select.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WValuesInsertSource : WInsertSource
    {
        public bool IsDefaultValues { get; set; }
        public IList<WRowValue> RowValues { get; set; }

        internal override bool OneLine()
        {
            return RowValues.Aggregate(true, (current, rowValue) => current && rowValue.OneLine());
        }

        internal override string ToString(string indent)
        {
            if (IsDefaultValues)
                return " DEFAULT VALUES ";
            else
            {
                var sb = new StringBuilder();
                sb.AppendFormat(" VALUES {0}\r\n", RowValues[0].ToString(indent));
                for (var i = 1; i < RowValues.Count; ++i)
                {
                    sb.AppendFormat("{0}\r\n", RowValues[i].ToString(indent));
                }
                return sb.ToString();
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (RowValues != null)
            {
                var index = 0;
                for (var count = RowValues.Count; index < count; ++index)
                    RowValues[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WRowValue : WSqlFragment
    {
        public IList<WScalarExpression> ColumnValues { get; set; }

        internal override bool OneLine()
        {
            return ColumnValues.Aggregate(true, (current, expr) => current && expr.OneLine());
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}({1}", indent, ColumnValues[0].ToString(indent));

            for (var i = 1; i < ColumnValues.Count; ++i)
            {
                sb.AppendFormat(", {0}", ColumnValues[i].ToString(indent));
            }
            sb.Append(")");

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (ColumnValues != null)
            {
                var index = 0;
                for (var count = ColumnValues.Count; index < count; ++index)
                    ColumnValues[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }
}
