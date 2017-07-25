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
using System;
using System.Collections.Generic;
using System.Globalization;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    public enum SortOrder
    {
        Ascending,
        Descending,
        NotSpecified,
    }

    public abstract partial class WGroupingSpecification : WSqlFragment { }

    public partial class WCompositeGroupingSpec : WGroupingSpecification
    {
        internal IList<WGroupingSpecification> Items { get; set; }

        internal override string ToString(string indent)
        {
            throw new NotImplementedException();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Items != null)
            {
                var index = 0;
                for (var count = Items.Count; index < count; ++index)
                    Items[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WExpressionGroupingSpec : WGroupingSpecification
    {
        internal WScalarExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            return Expression.ToString(indent);
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

    public partial class WExpressionWithSortOrder : WSqlFragment
    {
        internal WScalarExpression ScalarExpr { get; set; }
        internal SortOrder SortOrder { get; set; }

        internal override bool OneLine()
        {
            return ScalarExpr.OneLine();
        }

        internal override string ToString(string indent)
        {
            if (SortOrder == SortOrder.NotSpecified)
            {
                return ScalarExpr.ToString(indent);
            }
            else
            {
                return string.Format(CultureInfo.CurrentCulture, "{0} {1}",
                    ScalarExpr.ToString(indent),
                    TsqlFragmentToString.SortOrder(SortOrder));
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (ScalarExpr != null)
                ScalarExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }
}
