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
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// BooleanExprNormalizeVisitor traverses a boolean expression 
    /// and normalizes it to a list of conjunctive predicates.
    /// </summary>
    internal class BooleanExpressionNormalizeVisitor : WSqlFragmentVisitor
    {
        private List<WBooleanExpression> _normalizedList;

        public List<WBooleanExpression> Invoke(WBooleanExpression expr)
        {
            _normalizedList = new List<WBooleanExpression>();
            expr.Accept(this);

            return _normalizedList;
        }

        private void Extract(WBooleanExpression expr)
        {
            _normalizedList.Add(expr);
        }

        public override void Visit(WBooleanBinaryExpression node)
        {
            if (node.BooleanExpressionType == BooleanBinaryExpressionType.And)
            {
                base.Visit(node);
            }
            else
            {
                Extract(new WBooleanParenthesisExpression { Expression = node });
            }
        }

        public override void Visit(WBooleanComparisonExpression node)
        {
            Extract(node);
        }

        public override void Visit(WBooleanIsNullExpression node)
        {
            Extract(node);
        }

        public override void Visit(WBetweenExpression node)
        {
            Extract(node);
        }

        public override void Visit(WLikePredicate node)
        {
            Extract(node);
        }

        public override void Visit(WInPredicate node)
        {
            Extract(node);
        }

        public override void Visit(WSubqueryComparisonPredicate node)
        {
            Extract(node);
        }

        public override void Visit(WExistsPredicate node)
        {
            Extract(node);
        }

        public override void Visit(WBooleanNotExpression node)
        {
            Extract(node);
        }
    }


    /// <summary>
    /// ScalarExprTableReferenceVisitor traverses a scalar expression and returns all the tables and properties it references
    /// </summary>
    internal class ScalarExprTableReferenceVisitor : WSqlFragmentVisitor
    {
        private Dictionary<string, HashSet<string>> _tableandPropertiesDict;

        // <table name, properties referenced>
        public Dictionary<string, HashSet<string>> Invoke(WScalarExpression expr)
        {
            _tableandPropertiesDict = new Dictionary<string, HashSet<string>>();
            expr.Accept(this);

            return _tableandPropertiesDict;
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            var column = node.MultiPartIdentifier.Identifiers;

            if (column.Count >= 2)
            {
                var tableName = column.First().Value;
                var propertyName = "";
                HashSet<string> properties;
                if (!_tableandPropertiesDict.TryGetValue(tableName, out properties))
                    _tableandPropertiesDict[tableName] = new HashSet<string>();

                propertyName += column[1].Value;
                for (var i = 2; i < column.Count; i++)
                    propertyName += "." + column[i].ToString();
                _tableandPropertiesDict[tableName].Add(propertyName);
            }
            else
                throw new QueryCompilationException("Identifier " + column.ToString() + " must be bound to a table alias.");
        }
    }

    /// <summary>
    /// BooleanExprTableReferenceVisitor traverses a boolean expression and returns all the tables and properties it references
    /// </summary>
    internal class BooleanExprTableReferenceVisitor : WSqlFragmentVisitor
    {
        private Dictionary<string, HashSet<string>> _tableandPropertiesDict;

        // <table name, properties referenced>
        public Dictionary<string, HashSet<string>> Invoke(WBooleanExpression expr)
        {
            _tableandPropertiesDict = new Dictionary<string, HashSet<string>>();
            expr.Accept(this);

            return _tableandPropertiesDict;
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            var column = node.MultiPartIdentifier.Identifiers;

            if (column.Count >= 2 )
            {
                var tableName = column.First().Value;
                var propertyName = "";
                HashSet<string> properties;
                if (!_tableandPropertiesDict.TryGetValue(tableName, out properties))
                    _tableandPropertiesDict[tableName] = new HashSet<string>();
                propertyName += column[1].Value;
                for (var i = 2; i < column.Count; i++)
                    propertyName += "." + column[i].ToString();
                _tableandPropertiesDict[tableName].Add(propertyName);
            }
            else
                throw new GraphViewException("Identifier " + column.ToString() + " should be bound to a table.");
        }
    }

    /// <summary>
    /// AttachWhereClauseVisitor traverses the WHERE clause and attachs predicates
    /// into nodes and edges of constructed graph.
    /// </summary>
    internal class AttachWhereClauseVisitor : WSqlFragmentVisitor
    {
        private MatchGraph _graph;

        private readonly BooleanExprTableReferenceVisitor _booleanTabRefVisitor = new BooleanExprTableReferenceVisitor();
        internal List<WBooleanExpression> FailedToAssign = new List<WBooleanExpression>();

        public void Invoke(WWhereClause node, MatchGraph graph)
        {
            _graph = graph;

            if (node.SearchCondition != null)
                node.SearchCondition.Accept(this);
        }

        private void Attach(WBooleanExpression expr)
        {
            var table = _booleanTabRefVisitor.Invoke(expr);
            // Only expression who reference one table can be attached
            var tableName = table.Count == 1 ? table.First().Key : "";

            MatchEdge edge;
            MatchNode node;
            if (_graph.TryGetEdge(tableName, out edge))
            {
                if (edge.Predicates == null)
                {
                    edge.Predicates = new List<WBooleanExpression>();
                }
                edge.Predicates.Add(expr);
            }
            else if (_graph.TryGetNode(tableName, out node))
            {
                if (node.Predicates == null)
                {
                    node.Predicates = new List<WBooleanExpression>();
                }
                node.Predicates.Add(expr);
            }
            else FailedToAssign.Add(expr);
        }

        public override void Visit(WBooleanBinaryExpression node)
        {
            if (node.BooleanExpressionType == BooleanBinaryExpressionType.And)
            {
                base.Visit(node);
            }
            else
            {
                // TODO: Remove parenthesis
                Attach(new WBooleanParenthesisExpression { Expression = node });
            }
        }

        public override void Visit(WBooleanComparisonExpression node)
        {
            Attach(node);
        }

        public override void Visit(WBooleanIsNullExpression node)
        {
            Attach(node);
        }

        public override void Visit(WBetweenExpression node)
        {
            Attach(node);
        }

        public override void Visit(WLikePredicate node)
        {
            Attach(node);
        }

        public override void Visit(WInPredicate node)
        {
            Attach(node);
        }

        public override void Visit(WSubqueryComparisonPredicate node)
        {
            Attach(node);
        }

        public override void Visit(WExistsPredicate node)
        {
            Attach(node);
        }

        public override void Visit(WBooleanNotExpression node)
        {
            Attach(node);
        }
    }

    /// <summary>
    /// ModifyTableNameVisitor traverses a boolean expression and
    /// 1. change all the existing table name to _tableName
    /// 2. attach _tableName to all the columns not bound with any table
    /// </summary>
    internal class ModifyTableNameVisitor : WSqlFragmentVisitor
    {
        private string _tableName;

        public void Invoke(WBooleanExpression node, string tableName)
        {
            _tableName = tableName;
            node.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            var column = node.MultiPartIdentifier.Identifiers;
            if (column.Count >= 2 && _tableName.Equals(column.First().Value, StringComparison.OrdinalIgnoreCase))
            {
                column.First().Value = _tableName;
            }
            else
            {
                node.MultiPartIdentifier.Identifiers.Insert(0, new Identifier {Value = _tableName});
            }
        }

        public override void Visit(WScalarSubquery node)
        {
        }

        public override void Visit(WFunctionCall node)
        {
        }

        public override void Visit(WSearchedCaseExpression node)
        {
        }
    }

    #region comment codes
    //internal class CheckBooleanEqualExpersion : WSqlFragmentVisitor
    //{
    //    private WSqlTableContext _context;
    //    private MatchEdge _curPath;
    //    public void Invoke(MatchGraph graph, WSqlTableContext context)
    //    {
    //        _context = context;
    //        foreach (
    //            var path in
    //                graph.ConnectedSubGraphs.SelectMany(
    //                    e =>e.Edges.Values.Where(
    //                            ee => ee.Predicates != null && ee.IsPath)))
    //        {
    //            _curPath = path;
    //            foreach (var predicate in path.Predicates)
    //            {
    //                predicate.Accept(this);
    //            }
    //        }
    //    }
    //    public override void Visit(WBooleanBinaryExpression node)
    //    {
    //        if (node.BooleanExpressionType != BooleanBinaryExpressionType.And)
    //        {
    //            throw new GraphViewException("Only conjunction is allowed in path predicates");
    //        }
    //        base.Visit(node);

    //    }

    //    public override void Visit(WBooleanComparisonExpression node)
    //    {
    //        if (node.ComparisonType!=BooleanComparisonType.Equals)
    //            throw new GraphViewException("Only equal comparison expression between column and value is allowed in path predicates");
    //        WValueExpression valueExpression = node.FirstExpr as WValueExpression;
    //        WColumnReferenceExpression columnReferenceExpression;
    //        if (valueExpression==null)
    //        {
    //            valueExpression = node.SecondExpr as WValueExpression;
    //            if (valueExpression == null)
    //                throw new GraphViewException("Only equal comparison expression between column and value is allowed in path predicates");
    //            columnReferenceExpression = node.FirstExpr as WColumnReferenceExpression;
    //            if (columnReferenceExpression == null)
    //                throw new GraphViewException("Only equal comparison expression between column and value is allowed in path predicates");
    //        }
    //        else
    //        {
    //            columnReferenceExpression = node.SecondExpr as WColumnReferenceExpression;
    //            if (columnReferenceExpression == null)
    //                throw new GraphViewException("Only equal comparison expression between column and value is allowed in path predicates");
    //        }
    //        string attributeName = columnReferenceExpression.MultiPartIdentifier.Identifiers.Last().Value;
    //        string value = valueExpression.ToString();
    //        _context.AddPathPredicateValue(_curPath, attributeName, value);
    //    }

    //    public override void Visit(WBooleanIsNullExpression node)
    //    {
    //        throw new GraphViewException("Only equal comparison expression is allowed in path predicates");
    //    }

    //    public override void Visit(WBetweenExpression node)
    //    {
    //        throw new GraphViewException("Only equal comparison expression is allowed in path predicates");
    //    }

    //    public override void Visit(WLikePredicate node)
    //    {
    //        throw new GraphViewException("Only equal comparison expression is allowed in path predicates");
    //    }

    //    public override void Visit(WInPredicate node)
    //    {
    //        throw new GraphViewException("Only equal comparison expression is allowed in path predicates");
    //    }

    //    public override void Visit(WSubqueryComparisonPredicate node)
    //    {
    //        throw new GraphViewException("Only equal comparison expression is allowed in path predicates");
    //    }

    //    public override void Visit(WExistsPredicate node)
    //    {
    //        throw new GraphViewException("Only equal comparison expression is allowed in path predicates");
    //    }
    //}



    //internal class CheckNodeEdgeReferenceVisitor : WSqlFragmentVisitor
    //{
    //    private bool _referencedByNodeAndEdge;
    //    private MatchGraph _graph;
    //    private Dictionary<string, string> _columnTableMapping;

    //    public CheckNodeEdgeReferenceVisitor(MatchGraph graph, Dictionary<string, string> columnTableMapping)
    //    {
    //        _graph = graph;
    //        _columnTableMapping = columnTableMapping;
    //    }
    //    public bool Invoke(WBooleanExpression node)
    //    {
    //        _referencedByNodeAndEdge = true;
    //        node.Accept(this);
    //        return _referencedByNodeAndEdge;
    //    }
    //    public override void Visit(WColumnReferenceExpression node)
    //    {
    //        if (!_referencedByNodeAndEdge) 
    //            return;
    //        var column = node.MultiPartIdentifier.Identifiers;
    //        string tableAlias = "";
    //        if (column.Count >= 2)
    //        {
    //            tableAlias = column[column.Count - 2].Value;
    //        }
    //        else
    //        {
    //            var columnName = column.Last().Value;
    //            if ((_columnTableMapping.ContainsKey(columnName)))
    //            {
    //                tableAlias = _columnTableMapping[columnName];
    //            }
    //        }

    //        if (!_graph.ContainsNode(tableAlias))
    //        {
    //            _referencedByNodeAndEdge = false;
    //        }
    //    }

    //    public override void Visit(WScalarSubquery node)
    //    {
    //        _referencedByNodeAndEdge = false;
    //    }

    //    public override void Visit(WFunctionCall node)
    //    {
    //        _referencedByNodeAndEdge = false;
    //    }

    //    public override void Visit(WSearchedCaseExpression node)
    //    {
    //        _referencedByNodeAndEdge = false;
    //    }
    //}

    //internal class AttachNodeEdgePredictesVisitor : WSqlFragmentVisitor
    //{

    //    private CheckNodeEdgeReferenceVisitor _checkNodeEdgeReferenceVisitor;
    //    private WWhereClause _nodeEdgePredicatesWhenClause = new WWhereClause();

    //    public WWhereClause Invoke(WWhereClause node, MatchGraph graph, Dictionary<string, string> columnTableMapping)
    //    {
    //        _checkNodeEdgeReferenceVisitor = new CheckNodeEdgeReferenceVisitor(graph, columnTableMapping)
    //        ;
    //        if (node.SearchCondition != null)
    //            node.SearchCondition.Accept(this);
    //        return _nodeEdgePredicatesWhenClause;
    //    }

    //    public void UpdateWherClause(WWhereClause whereClause, WBooleanExpression node)
    //    {
    //        if (whereClause.SearchCondition == null)
    //            whereClause.SearchCondition = node;
    //        else
    //        {
    //            whereClause.SearchCondition = new WBooleanBinaryExpression
    //            {
    //                FirstExpr = whereClause.SearchCondition,
    //                SecondExpr = node,
    //                BooleanExpressionType = BooleanBinaryExpressionType.And
    //            };
    //        }
    //    }

    //    public override void Visit(WBooleanBinaryExpression node)
    //    {
    //        if (node.BooleanExpressionType == BooleanBinaryExpressionType.And)
    //        {
    //            if (_checkNodeEdgeReferenceVisitor.Invoke(node.FirstExpr))
    //            {
    //                UpdateWherClause(_nodeEdgePredicatesWhenClause, node.FirstExpr);
    //            }
    //            else
    //            {
    //                base.Visit(node.FirstExpr);
    //            }
    //            if (_checkNodeEdgeReferenceVisitor.Invoke(node.SecondExpr))
    //            {
    //                UpdateWherClause(_nodeEdgePredicatesWhenClause, node.SecondExpr);
    //            }
    //            else
    //            {
    //                base.Visit(node.SecondExpr);
    //            }
    //        }
    //        else
    //        {
    //            if (_checkNodeEdgeReferenceVisitor.Invoke(node))
    //            {
    //                UpdateWherClause(_nodeEdgePredicatesWhenClause,node);
    //            }
    //        }
    //    }

    //    public override void Visit(WBooleanComparisonExpression node)
    //    {
    //        if (_checkNodeEdgeReferenceVisitor.Invoke(node))
    //        {
    //            UpdateWherClause(_nodeEdgePredicatesWhenClause, node);
    //        }
    //    }

    //    public override void Visit(WBooleanIsNullExpression node)
    //    {
    //        if (_checkNodeEdgeReferenceVisitor.Invoke(node))
    //        {
    //            UpdateWherClause(_nodeEdgePredicatesWhenClause, node);
    //        }
    //    }

    //    public override void Visit(WBetweenExpression node)
    //    {
    //        if (_checkNodeEdgeReferenceVisitor.Invoke(node))
    //        {
    //            UpdateWherClause(_nodeEdgePredicatesWhenClause, node);
    //        }
    //    }

    //    public override void Visit(WLikePredicate node)
    //    {
    //        if (_checkNodeEdgeReferenceVisitor.Invoke(node))
    //        {
    //            UpdateWherClause(_nodeEdgePredicatesWhenClause, node);
    //        }
    //    }

    //    public override void Visit(WInPredicate node)
    //    {
    //    }

    //    public override void Visit(WSubqueryComparisonPredicate node)
    //    {
    //    }

    //    public override void Visit(WExistsPredicate node)
    //    {
    //    }
    //}
    #endregion


}
