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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// CollectVariableVisitor traverses a SQL fragment and get table names in this fragment.
    /// </summary>
    internal class CollectVariableVisitor : WSqlFragmentVisitor
    {
        private WSqlTableContext _results;
        /// <summary>
        /// List of Node tables tuples (Schema name, Table name)
        /// </summary>
        private IEnumerable<Tuple<string, string>> _nodeTables;

        public WSqlTableContext Invoke(WSqlFragment node, IEnumerable<Tuple<string, string>> nodeTables )
        {
            _results = new WSqlTableContext();
            _nodeTables = nodeTables;
            node.Accept(this);
            return _results;
        }

        public WSqlTableContext Invoke(WSqlFragment node)
        {
            _results = new WSqlTableContext();
            _nodeTables = null;
            node.Accept(this);
            return _results;
        }

        public override void Visit(WNamedTableReference node)
        {
            if (_nodeTables != null  &&
                !_nodeTables.Contains(WNamedTableReference.SchemaNameToTuple(node.TableObjectName)))
            {
                //throw new GraphViewException("Invalid Table " + node);
                return;
            }
            var name = node.Alias ?? node.TableObjectName.BaseIdentifier;
            _results.AddNodeTable(name.Value, node);
        }

        public override void Visit(WQueryDerivedTable node)
        {
            var name = node.Alias;
            _results.AddNodeTable(name.Value, node);
        }
    }
}