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
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Runtime.Remoting.Contexts;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    /// <summary>
    /// GetQueryTablesVisitor traverses a query and returns all tables in the expression.
    /// </summary>
    internal class GetQueryTablesVisitor : WSqlFragmentVisitor
    {
        private HashSet<string> _tables;
        private HashSet<string> _unboundColumns;

        public void Invoke(
            WSqlFragment node,
            out HashSet<string> tables,
            out HashSet<string> unboundColumns)
        {
            _tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _unboundColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            tables = _tables;
            unboundColumns = _unboundColumns;
            node.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            var columnExpr = node.MultiPartIdentifier.Identifiers;
            if (columnExpr.Count >= 2)
                _tables.Add(columnExpr[columnExpr.Count - 2].Value.ToLower(CultureInfo.CurrentCulture));
            else
                _unboundColumns.Add(columnExpr.Last().Value.ToLower(CultureInfo.CurrentCulture));
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
}
