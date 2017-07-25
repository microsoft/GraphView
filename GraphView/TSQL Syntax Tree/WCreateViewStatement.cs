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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// Syntax tree of a CREATE TABLE statement
    /// </summary>
    public class WCreateViewStatement : WSqlStatement
    {
        public IList<Identifier> Columns { get; set; }
 
        private WSchemaObjectName _schemaObjectName;

        public WSchemaObjectName SchemaObjectName
        {
            get { return _schemaObjectName; }
            set { UpdateTokenInfo(value); _schemaObjectName = value; }
        }

        public WSelectQueryExpression SelectStatement { get; set; }

        public IList<ViewOption> ViewOptions { get; set; }
        public bool WithCheckOption { get; set; }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            SelectStatement.Accept(visitor);
            base.AcceptChildren(visitor);
        }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0}CREATE VIEW {1} AS\r\n", indent, SchemaObjectName);
            if (Columns != null && Columns.Any())
            {
                sb.Append(" ");
                for (var i = 0; i < Columns.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append('.');
                    }
                    sb.Append("[" + Columns[i].Value + "]");
                }
                sb.Append("\r\n");
            }
            if (ViewOptions != null && ViewOptions.Any())
            {
                sb.Append("WITH ");
                for (var i = 0; i < ViewOptions.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append('.');
                    }
                    sb.Append("[" + ViewOptions[i].OptionKind+ "]");
                }
                sb.Append("\r\n");
            }
            sb.Append(SelectStatement);
            sb.Append("\r\n");
            if (WithCheckOption)
                sb.Append("WITH CHECK OPTION");
            
            return sb.ToString();
        }
    }
}
