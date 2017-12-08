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
using System.Linq;
using System.Text;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public enum BinaryQueryExpressionType
    {
        Except,
        Intersect,
        Union
    }

    public enum UniqueRowFilter
    {
        All,
        Distinct,
        NotSpecified
    }

    /// <summary>
    /// The base class of a SELECT statement
    /// </summary>
    public partial class WSelectStatement : WStatementWithCtesAndXmlNamespaces
    {
        // The table name of the INTO clause
        internal WSchemaObjectName Into { set; get; }

        // The body of the SELECT statement
        internal WSelectQueryExpression QueryExpr { set; get; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            if (Into != null)
            {
                sb.AppendFormat("{0}SELECT INTO {1}\r\n", indent, Into);
            }
            sb.Append(QueryExpr.ToString(indent));
            sb.Append(OptimizerHintListToString(indent));

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Into != null)
                Into.Accept(visitor);
            if (QueryExpr != null)
                QueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);


        }
    }

    /// <summary>
    /// The base class of the SELECT query hierarchy
    /// </summary>
    public abstract partial class WSelectQueryExpression : WSqlStatement
    {
        // Omit ForClause and OffsetClause

        internal WOrderByClause OrderByClause { set; get; }
        internal WSchemaObjectName Into { set; get; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (OrderByClause != null)
                OrderByClause.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// SELECT query within a parenthesis
    /// </summary>
    public partial class WQueryParenthesisExpression : WSelectQueryExpression
    {
        internal WSelectQueryExpression QueryExpr { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}(\r\n", indent);
            sb.AppendFormat("{0}\r\n", QueryExpr.ToString(indent));
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
            if (QueryExpr != null)
                QueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// Represents the union/except/intersect of SELECT queries.
    /// </summary>
    public partial class WBinaryQueryExpression : WSelectQueryExpression
    {
        // Indicates whether the ALL keyword is used in the binary SQL espression.
        internal bool All { set; get; }

        // The binary operation type: union, except or intersect
        internal BinaryQueryExpressionType BinaryQueryExprType { get; set; }

        internal WSelectQueryExpression FirstQueryExpr { get; set; }
        internal WSelectQueryExpression SecondQueryExpr { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}\r\n", FirstQueryExpr.ToString(indent));

            sb.AppendFormat(All ? "{0}{1} ALL\r\n" : "{0}{1}\r\n", indent,
                TsqlFragmentToString.BinaryQueryExpressionType(BinaryQueryExprType));

            sb.AppendFormat("{0}", SecondQueryExpr.ToString(indent));

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FirstQueryExpr != null)
                FirstQueryExpr.Accept(visitor);
            if (SecondQueryExpr != null)
                SecondQueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// The body of the SELECT query, including a list of selected elements, FROM and WHERE clauses
    /// </summary>
    public partial class WSelectQueryBlock : WSelectQueryExpression
    {
        internal IList<WSelectElement> SelectElements { get; set; }
        internal WFromClause FromClause { get; set; }
        internal WWhereClause WhereClause { get; set; }
        internal WTopRowFilter TopRowFilter { get; set; }
        internal WGroupByClause GroupByClause { get; set; }
        internal WHavingClause HavingClause { get; set; }
        internal WMatchClause MatchClause { get; set; }
        internal WLimitClause LimitClause { get; set; }
        internal WWithPathClause WithPathClause { get; set; }
        internal WWithPathClause2 WithPathClause2 { get; set; }
        internal UniqueRowFilter UniqueRowFilter { get; set; }
        internal bool OutputPath { get; set; }

        public WSelectQueryBlock()
        {
            SelectElements = new List<WSelectElement>();
        }

        internal override bool OneLine()
        {
            if (FromClause == null &&
                MatchClause == null &&
                WhereClause == null &&
                OrderByClause == null &&
                GroupByClause == null)
            {
                return SelectElements.All(sel => sel.OneLine());
            }
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            if (WithPathClause2 != null)
            {
                sb.Append(WithPathClause2.ToString(indent));
            }

            sb.AppendFormat("{0}SELECT ", indent);

            if (TopRowFilter != null)
            {
                if (TopRowFilter.OneLine())
                {
                    sb.AppendFormat("{0} ", TopRowFilter.ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.AppendFormat("{0} ", TopRowFilter.ToString(indent));
                }
            }

            switch (UniqueRowFilter)
            {
                case UniqueRowFilter.All:
                    sb.Append("ALL ");
                    break;
                case UniqueRowFilter.Distinct:
                    sb.Append("DISTINCT ");
                    break;
            }

            for (var i = 0; i < SelectElements.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                if (SelectElements[i].OneLine())
                {
                    sb.Append(SelectElements[i].ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(SelectElements[i].ToString(indent + " "));
                }
            }

            if (Into != null)
            {
                sb.AppendFormat(" INTO {0} ", Into);
            }

            if (FromClause != null)
            {
                sb.Append("\r\n");
                sb.Append(FromClause.ToString(indent));
            }

            if (MatchClause != null)
            {
                sb.Append("\r\n");
                sb.Append(MatchClause.ToString(indent));
            }

            if (WhereClause != null && (WhereClause.SearchCondition != null || !string.IsNullOrEmpty(WhereClause.GhostString)))
            {
                sb.Append("\r\n");
                sb.Append(WhereClause.ToString(indent));
            }

            if (GroupByClause != null)
            {
                sb.Append("\r\n");
                sb.Append(GroupByClause.ToString(indent));
            }

            if (HavingClause != null)
            {
                sb.Append("\r\n");
                sb.Append(HavingClause.ToString(indent));
            }

            if (OrderByClause != null)
            {
                sb.Append("\r\n");
                sb.Append(OrderByClause.ToString(indent));
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
            if (FromClause != null)
                FromClause.Accept(visitor);
            if (MatchClause != null)
                MatchClause.Accept(visitor);
            if (WhereClause != null)
                WhereClause.Accept(visitor);
            if (TopRowFilter != null)
                TopRowFilter.Accept(visitor);
            if (GroupByClause != null)
                GroupByClause.Accept(visitor);
            if (HavingClause != null)
                HavingClause.Accept(visitor);

            if (SelectElements != null)
            {
                var index = 0;
                for (var count = SelectElements.Count; index < count; ++index)
                    SelectElements[index].Accept(visitor);
            }

            base.AcceptChildren(visitor);
        }


    }

    public partial class WSelectQueryBlockWithMatchClause : WSelectQueryBlock
    {

    }

    public partial class WTopRowFilter : WSqlFragment
    {
        internal bool Percent { set; get; }
        internal bool WithTies { get; set; }
        internal WScalarExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(32);

            sb.AppendFormat("{0}TOP ", indent);

            if (Expression.OneLine())
            {
                sb.Append(Expression.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(Expression.ToString(indent + "  "));
            }

            if (Percent)
            {
                sb.Append(" PERCENT");
            }

            if (WithTies)
            {
                sb.Append(" WITH TIES");
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
            base.AcceptChildren(visitor);
        }
    }

    public partial class WWithPathClause : WSqlStatement
    {
        // Definition of a path: 
        // item1 is the binding name
        // item2 is the path description
        // item3 is the length limitation of it (-1 for no limitation)
        internal List<Tuple<string, WSelectQueryBlock, int>> Paths;
        internal List<Tuple<string, GraphViewExecutionOperator, int>> PathOperators;

        public WWithPathClause(List<Tuple<string, WSelectQueryBlock, int>> pPaths)
        {
            Paths = pPaths;
            PathOperators = new List<Tuple<string, GraphViewExecutionOperator, int>>();
        }

        public WWithPathClause(Tuple<string, WSelectQueryBlock, int> path)
        {
            PathOperators = new List<Tuple<string, GraphViewExecutionOperator, int>>();
            if (Paths == null) Paths = new List<Tuple<string, WSelectQueryBlock, int>>();
            Paths.Add(path);
        }

    }

    public partial class WWithPathClause2 : WSqlFragment
    {

        internal Dictionary<string, GraphViewExecutionOperator> PathOperators;
        internal Dictionary<string, WRepeatPath> Paths;

        public WWithPathClause2(Dictionary<string, WRepeatPath> pPaths)
        {
            Paths = pPaths;
            PathOperators = new Dictionary<string, GraphViewExecutionOperator>();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(32);

            if (Paths.Count > 0)
            {
                sb.AppendFormat("{0}With ", indent);
                sb.Append(Paths.ElementAt(0).Key + " AS " + "\r\n" + Paths.ElementAt(0).Value.ToString());
                for (var i = 1; i < Paths.Count; i++)
                {
                    sb.Append("\r\n");
                    sb.Append(", " + Paths.ElementAt(i).Key + " AS " + "\r\n" + Paths.ElementAt(i).Value.ToString());
                }

                sb.Append("\r\n");
            }
            return sb.ToString();
        }


    }

}

