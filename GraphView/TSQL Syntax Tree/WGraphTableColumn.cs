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
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public enum WNodeTableColumnRole
    {
        Property = 0,
        Edge = 1,
        NodeId = 2,
        EdgeView = 3,
        NodeViewProperty = 4
    }
    public enum WNodeTableRole
    {
        NodeTable = 0,
        NodeView = 1
    }
    /// <summary>
    /// Stores information of a column in a graph table, including column type, name, and its data type.
    /// </summary>
    public abstract partial class WNodeTableColumn : WSqlFragment
    {
        public abstract WNodeTableColumnRole ColumnRole { get; }
        public Identifier ColumnName { get; set; }
        public WDataTypeReference DataType { get; set; }
    }
    public enum WEdgeAttributeType
    {
        Int,
        Long,
        Double,
        String,
        Bool
    }

    public partial class WGraphTableEdgeColumn : WNodeTableColumn
    {
        public override WNodeTableColumnRole ColumnRole
        {
            get { return WNodeTableColumnRole.Edge; }
        }

        public IList<Tuple<Identifier, WEdgeAttributeType>> Attributes { get; set; }

        internal WTableReference TableReference { get; set; }
    }

    public partial class WGraphTablePropertyColumn : WNodeTableColumn
    {
        public override WNodeTableColumnRole ColumnRole
        {
            get { return WNodeTableColumnRole.Property; }
        }
    }

    public partial class WGraphTableNodeIdColumn : WGraphTablePropertyColumn
    {
        public override WNodeTableColumnRole ColumnRole
        {
            get { return WNodeTableColumnRole.NodeId; }
        }
    }
}
