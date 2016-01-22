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
    /// Checks if any columns of a specific table are referenced in a statement,
    /// and removes
    /// </summary>
    class CheckTableReferenceVisitor : WSqlFragmentVisitor
    {
        private bool _tableExists;
        private string _tableName;
        private WNamedTableReference _tableRef;
        private Dictionary<string, string> _columnTableMapping; 

        /// <summary>
        /// Returns true if there are columns of a specific table referenced in a statement
        /// </summary>
        /// <param name="node">The query statement</param>
        /// <param name="tableName">the table for query</param>
        /// <param name="context">Sql context with table alias mapping</param>
        /// <param name="columnsOfNodeTables"></param>
        /// <param name="conn">A open Sql connection</param>
        /// <returns></returns>
        public bool Invoke(WSelectQueryBlock node, string tableName, WSqlTableContext context,
            Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>> columnsOfNodeTables)
        {
            _tableExists = false;
            _tableName = tableName;
            _tableRef = context[tableName] as WNamedTableReference;
            _columnTableMapping = context.GetColumnToAliasMapping(columnsOfNodeTables);
            node.Accept(this);
            return _tableExists;
        }

        //public override void Visit(WSelectQueryBlock node)
        //{
        //    if (node.SelectElements.Any(e => e is WSelectStarExpression))
        //        _tableExists = true;
        //    else
        //        base.Visit(node);
        //}

        public override void Visit(WSelectStarExpression node)
        {
            if (_tableExists)
                return;
            if (node.Qulifier == null)
            {
                _tableExists = true;
                return;
            }
            if (String.Equals(node.Qulifier.Identifiers.Last().Value, _tableName, StringComparison.OrdinalIgnoreCase))
                _tableExists = true;
            
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            if (_tableExists)
                return;
            if (node.MultiPartIdentifier == null) return;
            var columnIdentifiers = node.MultiPartIdentifier.Identifiers;
            var columnName = columnIdentifiers.Last().Value;
            switch (columnIdentifiers.Count)
            {
                // unbound column, no need to check because all unbounded columns have been bounded to a table when constructing the graph
                case 1:
                    if (_columnTableMapping.ContainsKey(columnName) &&
                        String.Equals(_columnTableMapping[columnName], _tableName,
                            StringComparison.OrdinalIgnoreCase))
                        _tableExists = true;
                    break;
                // column referencd by exposed name
                case 2:
                    var tableExposedName = columnIdentifiers[0].Value;
                    if (String.Equals(tableExposedName, _tableName, StringComparison.OrdinalIgnoreCase))
                        _tableExists = true;
                    break;
                // column referencd by complete table name
                default:
                    var flag = true;
                    var index1 = columnIdentifiers.Count - 2;
                    var index2 = _tableRef.TableObjectName.Count - 1;
                    for (; index1 >= 0 && index2 >= 0; --index1, --index2)
                    {
                        if (String.Equals(columnIdentifiers[index1].Value, _tableRef.TableObjectName[index2].Value,
                            StringComparison.OrdinalIgnoreCase))
                            continue;
                        flag = false;
                        break;
                    }
                    _tableExists = flag;
                    break;
            }
        }
    }
}
