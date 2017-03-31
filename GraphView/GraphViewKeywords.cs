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
using System.Reflection;
using System.Runtime.CompilerServices;

namespace GraphView
{
    internal static class GraphViewKeywords
    {
        public static HashSet<string> _keywords { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public const string KW_DOC_ID = "id";
        public const string KW_DOC_PARTITION = "_partition";
        public const string KW_DOC_ETAG = "_etag";

        public const string KW_VERTEX_LABEL = "label";
        public const string KW_VERTEX_EDGE = GremlinKeyword.EdgeAdj;
        public const string KW_VERTEX_REV_EDGE = GremlinKeyword.ReverseEdgeAdj;
        //public const string KW_VERTEX_NEXTOFFSET = "_nextEdgeOffset";
        public const string KW_VERTEX_EDGE_SPILLED = "_edgeSpilled";
        public const string KW_VERTEX_REVEDGE_SPILLED = "_revEdgeSpilled";

        public const string KW_PROPERTY_ID = "id";
        public const string KW_PROPERTY_VALUE = "_value";
        public const string KW_PROPERTY_META = "_meta";

        public const string KW_EDGE_LABEL = "label";
        public const string KW_EDGE_ID = "id";
        //public const string KW_EDGE_OFFSET = "_offset";
        public const string KW_EDGE_SRCV = "_srcV";
        public const string KW_EDGE_SRCV_LABEL = "_srcVLabel";
        public const string KW_EDGE_SINKV = "_sinkV";
        public const string KW_EDGE_SINKV_LABEL = "_sinkVLabel";

        public const string KW_EDGEDOC_VERTEXID = "_vertex_id";
        public const string KW_EDGEDOC_ISREVERSE = "_is_reverse";
        public const string KW_EDGEDOC_EDGE = KW_VERTEX_EDGE;

        public static string KW_TABLE_DEFAULT_COLUMN_NAME = GremlinKeyword.TableDefaultColumnName;

        internal enum Pop
        {
            All,
            First,
            Last
        }

        static GraphViewKeywords()
        {
#if !DEBUG
            Type thisType = typeof(GraphViewKeywords);
            foreach (FieldInfo field in thisType.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)) {
                if (field.FieldType == typeof(string)) {
                    _keywords.Add((string) field.GetValue(null));
                }
            }
#endif
        }
    }
}
