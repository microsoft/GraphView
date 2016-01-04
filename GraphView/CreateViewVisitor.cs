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
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class EdgeViewSelectStatementVisitor : WSqlFragmentVisitor
    {
        private string _schema;
        private List<Tuple<string, string>> _edges;
        private List<string> _edgeAttribute;
        private List<Tuple<string, List<Tuple<string, string, string>>>> _attributeMapping;
        public void Invoke(string schema, WSelectQueryExpression selectStatement, out List<Tuple<string, string>> edges, out List<string> edgeAttribute, out List<Tuple<string, List<Tuple<string, string, string>>>> attributeMapping)
        {
            _schema = schema;
            _edges = new List<Tuple<string, string>>();
            _edgeAttribute = new List<string>();
            _attributeMapping = new List<Tuple<string, List<Tuple<string, string, string>>>>();
            selectStatement.Accept(this);
            edges = _edges;
            edgeAttribute = _edgeAttribute;
            attributeMapping = _attributeMapping;
        }

        public override void Visit(WBinaryQueryExpression node)
        {
            if (node.BinaryQueryExprType != BinaryQueryExpressionType.Union)
                throw new NodeViewException("Only UNION ALL can be used in Select Statement.");
            base.Visit(node);
        }

        private void UpdateMapping(WSelectQueryBlock expr)
        {
            if (expr == null)
                throw new NodeViewException("Invalid Select Statement in CREATE NODE VIEW");
            if (expr.FromClause.TableReferences.Count != 1)
                throw new NodeViewException(
                    "Only one node table can be specified in each select statemtn of CREATE NODE VIEW");

            var tableRef = expr.FromClause.TableReferences.First() as WNamedTableReference;
            if (tableRef == null)
                throw new NodeViewException("Invalid node table");

            var schema = tableRef.TableObjectName.DatabaseIdentifier == null
                ? "dbo"
                : tableRef.TableObjectName.DatabaseIdentifier.Value;
            if (string.Compare(schema, _schema, StringComparison.CurrentCultureIgnoreCase) != 0)
                throw new NodeViewException("All the node tables should be in the same schema as the node view");

            var tableRefName = tableRef.TableObjectName.SchemaIdentifier.Value;
            var edgeName = tableRef.TableObjectName.BaseIdentifier.Value;
            _edges.Add(new Tuple<string, string>(tableRefName, edgeName));
            if (!_attributeMapping.Any())
            {
                foreach (var selectElement in expr.SelectElements)
                {
                    var scalarElement = selectElement as WSelectScalarExpression;
                    if (scalarElement == null)
                        throw new NodeViewException("Invalid select element");
                    string newColName = scalarElement.ColumnName;
                    var oldColRefExpression = scalarElement.SelectExpr as WColumnReferenceExpression;
                    if (oldColRefExpression == null)
                    {
                        var oldColValueExpression = scalarElement.SelectExpr as WValueExpression;
                        if (oldColValueExpression == null || oldColValueExpression.Value.ToLower() != "null" ||
                            string.IsNullOrEmpty(newColName))
                            throw new NodeViewException("Each select element should be null or reference to a column");
                        _attributeMapping.Add(
                            new Tuple<string, List<Tuple<string, string, string>>>(newColName.ToLower(),
                                new List<Tuple<string, string, string>>()));
                    }
                    else
                    {
                        string oldColName = oldColRefExpression.MultiPartIdentifier.Identifiers.Last().Value;
                        if (string.IsNullOrEmpty(newColName))
                            newColName = oldColName;
                        _attributeMapping.Add(new Tuple<string, List<Tuple<string,string, string>>>(newColName.ToLower(),
                            new List<Tuple<string, string, string>>
                            {
                                new Tuple<string, string, string>(tableRefName.ToLower(),edgeName.ToLower(), oldColName.ToLower())
                            }));
                    }
                    _edgeAttribute.Add(newColName);
                }
            }
            else
            {
                for (int i = 0; i < expr.SelectElements.Count; i++)
                {
                    var scalarElement = expr.SelectElements[i] as WSelectScalarExpression;
                    if (scalarElement == null)
                        throw new NodeViewException("Invalid select element");
                    var oldColRefExpression = scalarElement.SelectExpr as WColumnReferenceExpression;
                    if (oldColRefExpression == null)
                    {
                        var oldColValueExpression = scalarElement.SelectExpr as WValueExpression;
                        if (oldColValueExpression == null || oldColValueExpression.Value.ToLower() != "null")
                            throw new NodeViewException("Each select element should be null or reference to a column");
                    }
                    else
                    {
                        string oldColName = oldColRefExpression.MultiPartIdentifier.Identifiers.Last().Value;
                        _attributeMapping[i].Item2.Add(new Tuple<string, string, string>(tableRefName.ToLower(),
                            edgeName.ToLower(),
                            oldColName.ToLower()));
                    }
                }
            }
        }

        public override void Visit(WSelectQueryBlock node)
        {
            UpdateMapping(node);
        }
    }

    internal class NodeViewSelectStatementVisitor : WSqlFragmentVisitor
    {
        private string _schema;
        private List<string> _tableObjList;
        private List<Tuple<string, List<Tuple<string, string>>>> _propertymapping;

        public void Invoke(string schema, WSelectQueryExpression selectQuery,
            out List<string> tableObjList,
            out List<Tuple<string, List<Tuple<string, string>>>> propertymapping)
        {
            _schema = schema;
            _tableObjList = new List<string>();
            _propertymapping = new List<Tuple<string, List<Tuple<string, string>>>>();
            selectQuery.Accept(this);
            tableObjList = _tableObjList;
            propertymapping = _propertymapping;
        }


        private void UpdateMapping(WSelectQueryBlock expr)
        {
            if (expr == null)
                throw new NodeViewException("Invalid Select Statement in CREATE NODE VIEW");
            if (expr.FromClause.TableReferences.Count != 1)
                throw new NodeViewException(
                    "Only one node table can be specified in each select statemtn of CREATE NODE VIEW");
            
            var tableRef = expr.FromClause.TableReferences.First() as WNamedTableReference;
            if (tableRef == null)
                throw new NodeViewException("Invalid node table");
            
            var schema = tableRef.TableObjectName.SchemaIdentifier == null
                ? "dbo"
                : tableRef.TableObjectName.SchemaIdentifier.Value;
            if (string.Compare(schema, _schema, StringComparison.CurrentCultureIgnoreCase) != 0)
                throw new NodeViewException("All the node tables should be in the same schema as the node view");
            
            var tableRefName = tableRef.TableObjectName.BaseIdentifier.Value;
            _tableObjList.Add(tableRefName);
            if (!_propertymapping.Any())
            {
                foreach (var selectElement in expr.SelectElements)
                {
                    var scalarElement = selectElement as WSelectScalarExpression;
                    if (scalarElement == null)
                        throw new NodeViewException("Invalid select element");
                    string newColName = scalarElement.ColumnName;
                    var oldColRefExpression = scalarElement.SelectExpr as WColumnReferenceExpression;
                    if (oldColRefExpression == null)
                    {
                        var oldColValueExpression = scalarElement.SelectExpr as WValueExpression;
                        if (oldColValueExpression == null || oldColValueExpression.Value.ToLower() != "null" ||
                            string.IsNullOrEmpty(newColName))
                            throw new NodeViewException("Each select element should be null or reference to a column");
                        _propertymapping.Add(new Tuple<string, List<Tuple<string, string>>>(newColName.ToLower(),
                            new List<Tuple<string, string>>()));
                    }
                    else
                    {
                        string oldColName = oldColRefExpression.MultiPartIdentifier.Identifiers.Last().Value;
                        if (string.IsNullOrEmpty(newColName))
                            newColName = oldColName;
                        _propertymapping.Add(new Tuple<string, List<Tuple<string, string>>>(newColName.ToLower(),
                            new List<Tuple<string, string>>
                            {
                                new Tuple<string, string>(tableRefName.ToLower(), oldColName.ToLower())
                            }));
                    }
                }
            }
            else
            {
                for (int i = 0; i < expr.SelectElements.Count; i++)
                {
                    var scalarElement = expr.SelectElements[i] as WSelectScalarExpression;
                    if (scalarElement == null)
                        throw new NodeViewException("Invalid select element");
                    var oldColRefExpression = scalarElement.SelectExpr as WColumnReferenceExpression;
                    if (oldColRefExpression == null)
                    {
                        var oldColValueExpression = scalarElement.SelectExpr as WValueExpression;
                        if (oldColValueExpression == null || oldColValueExpression.Value.ToLower() != "null")
                            throw new NodeViewException("Each select element should be null or reference to a column");
                    }
                    else
                    {
                        string oldColName = oldColRefExpression.MultiPartIdentifier.Identifiers.Last().Value;
                        _propertymapping[i].Item2.Add(new Tuple<string, string>(tableRefName.ToLower(),
                            oldColName.ToLower()));
                    }
                }
            }
        }

        public override void Visit(WBinaryQueryExpression node)
        {
            if (node.BinaryQueryExprType!=BinaryQueryExpressionType.Union)
                throw new NodeViewException("Only UNION ALL can be used in Select Statement.");
            base.Visit(node);
        }

        public override void Visit(WSelectQueryBlock node)
        {
            UpdateMapping(node);
        }
    }
}
