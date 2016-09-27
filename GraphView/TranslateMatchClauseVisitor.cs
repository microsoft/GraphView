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

namespace GraphView
{

    public class NodeColumns
    {
        public WNodeTableColumnRole Role;
        public EdgeInfo EdgeInfo;
    }

    public class EdgeInfo
    {
        public HashSet<string> SinkNodes;
        public List<Tuple<string, string>> EdgeColumns;
        public IList<string> ColumnAttributes;
        public bool HasReversedEdge;
        public bool IsReversedEdge;
        public string EdgeUdfPrefix;
    }

    public class GraphMetaData
    {
        // Columns of each node table. For edge columns, edge attributes are attached.
        // (Schema name, Table name) -> (Column name -> Column Info)
        public readonly Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>> ColumnsOfNodeTables =
            new Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>>();

        // Node tables included in the node view.
        // (Schema name, Table name) -> set of the node table name included in the node view
        public readonly Dictionary<Tuple<string, string>, HashSet<string>> NodeViewMapping =
            new Dictionary<Tuple<string, string>, HashSet<string>>();

    }
}
