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
using System.Linq;
using System.Text;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// The FROM clause consists of a list of table references.
    /// </summary>
    public partial class WFromClause : WSqlFragment
    {
        internal IList<WTableReference> TableReferences { get; set; }

        public WFromClause()
        {
            TableReferences = new List<WTableReference>();
        }

        internal override bool OneLine()
        {
            return TableReferences.All(tref => tref.OneLine());
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.AppendFormat("{0}FROM ", indent);

            for (var i = 0; i < TableReferences.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }
                
                if (TableReferences[i].OneLine())
                {
                    sb.Append(TableReferences[i].ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(TableReferences[i].ToString(indent + "  "));
                }
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
            if (TableReferences != null)
            {
                var index = 0;
                for (var count = TableReferences.Count; index < count; ++index)
                    TableReferences[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// The WHERE clause consists of boolean expressions
    /// </summary>
    public partial class WWhereClause : WSqlFragment
    {
        internal WBooleanExpression SearchCondition { get; set; }

        //TODO: Use a Better Design, Serialize
        internal String GhostString { get; set; }

        internal new bool OneLine()
        {
            return SearchCondition.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.AppendFormat("{0}WHERE ", indent);

            if (SearchCondition != null)
            {
                if (SearchCondition.OneLine())
                {
                    sb.Append(SearchCondition.ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.AppendFormat(CultureInfo.CurrentCulture, SearchCondition.ToString(indent + " "));
                }
            }

            if (GhostString!=null)
                return sb.ToString() + GhostString;
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (SearchCondition != null)
                SearchCondition.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }


    public partial class WOrderByClause : WSqlFragment
    {
        internal IList<WExpressionWithSortOrder> OrderByElements { get; set; }

        internal override bool OneLine()
        {
            return OrderByElements.All(oe => oe.OneLine());
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);
            var newIndent = indent + "    ";

            sb.AppendFormat("{0}ORDER BY ", indent);

            for (var i = 0; i < OrderByElements.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                if (OrderByElements[i].OneLine())
                {
                    sb.Append(OrderByElements[i].ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.AppendFormat(CultureInfo.CurrentCulture, OrderByElements[i].ToString(indent + " "));
                }
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
            if (OrderByElements != null)
            {
                var index = 0;
                for (var count = OrderByElements.Count; index < count; ++index)
                    OrderByElements[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WGroupByClause : WSqlFragment
    {
        internal IList<WGroupingSpecification> GroupingSpecifications { get; set; }

        internal override bool OneLine()
        {
            return GroupingSpecifications.All(gspec => gspec.OneLine());
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(512);

            sb.AppendFormat("{0}GROUP BY ", indent);

            for (var i = 0; i < GroupingSpecifications.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                if (GroupingSpecifications[i].OneLine())
                {
                    sb.Append(GroupingSpecifications[i].ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(GroupingSpecifications[i].ToString(indent + " "));
                }
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
            if (GroupingSpecifications != null)
            {
                var index = 0;
                for (var count = GroupingSpecifications.Count; index < count; ++index)
                    GroupingSpecifications[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }
    }

    public partial class WHavingClause : WSqlFragment
    {
        internal WBooleanExpression SearchCondition { get; set; }

        internal override bool OneLine()
        {
            return SearchCondition.OneLine();
        }

        internal override string ToString(string indent)
        {
            return SearchCondition.OneLine() ? string.Format(CultureInfo.CurrentCulture, "{0}HAVING {1}", indent, SearchCondition.ToString(""))
                : string.Format(CultureInfo.CurrentCulture, "{0}HAVING \r\n{1}", indent, SearchCondition.ToString(indent + " "));
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (SearchCondition != null)
                SearchCondition.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public abstract partial class WWhenClause : WSqlFragment
    {
        internal WScalarExpression ThenExpression { get; set; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (ThenExpression != null)
                ThenExpression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WSearchedWhenClause : WWhenClause
    {
        internal WBooleanExpression WhenExpression { get; set; }

        internal override bool OneLine()
        {
            return WhenExpression.OneLine() && ThenExpression.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(128);

            sb.AppendFormat("{0}WHEN ", indent);

            if (WhenExpression.OneLine())
            {
                sb.Append(WhenExpression.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(WhenExpression.ToString(indent + " "));
            }

            sb.Append(" THEN ");

            if (ThenExpression.OneLine())
            {
                sb.Append(ThenExpression.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(ThenExpression.ToString(indent + " "));
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
            if (WhenExpression != null)
                WhenExpression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public abstract partial class WSetClause : WSqlFragment { }

    public partial class WFunctionCallSetClause : WSetClause
    {
        internal WFunctionCall MutatorFuction { get; set; }

        internal override string ToString(string indent)
        {
            return MutatorFuction.ToString(indent);
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (MutatorFuction != null)
                MutatorFuction.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WAssignmentSetClause : WSetClause
    {
        internal AssignmentKind AssignmentKind { get; set; }
        internal WColumnReferenceExpression Column { get; set; }
        internal WScalarExpression NewValue { get; set; }
        internal string Variable { get; set; }

        internal override bool OneLine()
        {
            return (NewValue != null && NewValue.OneLine()) || (Variable != null);
        }

        internal override string ToString(string indent)
        {
            string str = null;
            if (Column != null)
            {
                str = string.Format(CultureInfo.CurrentCulture, "{0} {1} ", Column.ToString(indent), TsqlFragmentToString.AssignmentType(AssignmentKind));

                if (OneLine())
                {
                    if (NewValue != null)
                    {
                        str += NewValue.ToString("");
                    }
                    else if (Variable != null)
                    {
                        str += Variable;
                    }
                }
                else
                {
                    if (NewValue != null)
                    {
                        str = string.Format(CultureInfo.CurrentCulture, "{0}\r\n{1}", str, NewValue.ToString(indent + " "));
                    }
                    else if (Variable != null)
                    {
                        str = string.Format(CultureInfo.CurrentCulture, "{0}\r\n{1}{2}", str, indent, Variable);
                    }
                }
            }
            else
            {
                str = string.Format(CultureInfo.CurrentCulture, "{0} {1} ", Variable, TsqlFragmentToString.AssignmentType(AssignmentKind));

                str = str + NewValue.ToString(indent);
            }
            return str;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Column != null)
                Column.Accept(visitor);
            if (NewValue != null)
                NewValue.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WMatchPath : WSqlFragment
    {
        /// <summary>
        /// A list of edges in the path expression, each in a pair of (source node table reference, edge column reference)
        /// </summary>
        internal IList<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>> PathEdgeList { get; set; }

        /// <summary>
        /// The tail of the path expression
        /// </summary>
        internal WSchemaObjectName Tail { get; set; }

        /// <summary>
        /// True if paths in PathEdgeList are reversed edges
        /// </summary>
        internal bool IsReversed { get; set; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (PathEdgeList != null)
            {
                var index = 0;
                for (var count = PathEdgeList.Count; index < count; ++index)
                {
                    PathEdgeList[index].Item1.Accept(visitor);
                    PathEdgeList[index].Item2.Accept(visitor);
                }
            }

            Tail?.Accept(visitor);

            base.AcceptChildren(visitor);
        }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(64);

            sb.Append(indent);
            for (int i = 0; i < PathEdgeList.Count; i++)
            {
                string arrowSource;
                string arrowSink;
                switch (PathEdgeList[i].Item2.EdgeType)
                {
                    case WEdgeType.BothEdge:
                        arrowSource = "-";
                        arrowSink = "-";
                        break;
                    case WEdgeType.InEdge:
                        arrowSource = "<-";
                        arrowSink = "-";
                        break;
                    case WEdgeType.OutEdge:
                        arrowSource = "-";
                        arrowSink = "->";
                        break;
                    default:
                        arrowSource = "-";
                        arrowSink = "->";
                        break;
                }
                sb.AppendFormat("{0}{1}", PathEdgeList[i].Item1.BaseIdentifier.Value, arrowSource);
                sb.AppendFormat("[{0}]", PathEdgeList[i].Item2);
                if (Tail != null)
                {
                    sb.Append(arrowSink);
                }
            }
            if (Tail != null)
            {
                sb.Append(Tail.BaseIdentifier.Value);
            }
            return sb.ToString();
        }
    }
    public partial class WMatchClause : WSqlFragment
    {
        internal IList<WMatchPath> Paths { get; set; }

        public WMatchClause()
        {
            Paths = new List<WMatchPath>();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Paths != null)
            {
                var index = 0;
                for (var count = Paths.Count; index < count; ++index)
                    Paths[index].Accept(visitor);
            }
            base.AcceptChildren(visitor);
        }

        internal override bool OneLine()
        {
            return Paths != null && Paths.Count == 1;
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(128);

            sb.AppendFormat("{0}MATCH ", indent);
            if (Paths.Count > 0)
            {
                sb.AppendFormat("{0}", Paths[0].ToString());

                for (int i = 1; i < Paths.Count; i++)
                {
                    sb.Append("\r\n");
                    sb.AppendFormat("  {0}{1}", indent, Paths[i].ToString());
                }
            }

            return sb.ToString();
        }
    }

    public partial class WLimitClause : WSqlFragment
    {
        internal int Limit { get; set; }
    }

}
