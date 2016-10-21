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
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public partial class WIndexDefinition : WSqlFragment
    {
        public IList<Tuple<WColumnReferenceExpression, SortOrder>> Columns { get; set; }
        public IndexType IndexType { get; set; }
        private Identifier _name;

        public Identifier Name
        {
            get { return _name; }
            set { UpdateTokenInfo(value); _name = value; }
        }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}INDEX {1}", indent, Name.Value);
            if (IndexType.IndexTypeKind != null)
            {
                if (IndexType.IndexTypeKind == IndexTypeKind.Clustered)
                    sb.Append(" CLUSTERED");
                if (IndexType.IndexTypeKind == IndexTypeKind.NonClustered)
                    sb.Append(" NONCLUSTERED");
            }
            if (Columns == null || Columns.Count <= 0)
                return sb.ToString();
            sb.Append(" (");
            for (var i = 0; i < Columns.Count; ++i)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append(Columns[i].Item1);
                switch (Columns[i].Item2)
                {
                    case SortOrder.Ascending:
                        sb.Append(" ASC");
                        break;
                    case SortOrder.Descending:
                        sb.Append(" DESC");
                        break;
                }
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
}
