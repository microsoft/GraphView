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
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
 
    public partial class WTableDefinition : WSqlFragment
    {
        public IList<WColumnDefinition> ColumnDefinitions { get; set; }
        public IList<WConstraintDefinition> TableConstraints { get; set; }
        public IList<WIndexDefinition> Indexes { get; set; }
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            bool first = true;
            foreach (var t in ColumnDefinitions)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.Append(",\r\n");
                }
                sb.AppendFormat("{0}{1}", indent, t);

            }
            foreach (var t in TableConstraints)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.Append(",\r\n");
                }
                sb.AppendFormat("{0}{1}", indent, t);
            }
            foreach (var t in Indexes)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    sb.Append(",\r\n");
                }
                sb.AppendFormat("{0}{1}", indent, t);
            }
            return sb.ToString();
        }
    }

    public partial class WDeclareTableVariable : WSqlStatement
    {
        public WTableDefinition Definition { get; set; }
        public Identifier VariableName { get; set; }
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0}DECLARE {1} TABLE\n",indent, VariableName.Value);
            sb.AppendFormat("{0}(\r\n{1})\r\n", indent, Definition.ToString(indent + " "));
            return sb.ToString();
        }

    }
}
