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
using System.Text;

namespace GraphView
{
    /// <summary>
    /// Syntax tree of a CREATE TABLE statement
    /// </summary>
    public class WCreateTableStatement : WSqlStatement
    {
        private WSchemaObjectName _schemaObjectName;
        private WTableDefinition _definition;

        public WSchemaObjectName SchemaObjectName
        {
            get { return _schemaObjectName; }
            set { UpdateTokenInfo(value); _schemaObjectName = value; }
        }


        public WTableDefinition Definition
        {
            get { return _definition; }
            set { UpdateTokenInfo(value); _definition = value; }
        }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);
            sb.AppendFormat("{0}CREATE TABLE {1}\r\n", indent, SchemaObjectName);
            if (_definition != null)
            {
                sb.AppendFormat("{0}(\r\n{1})\r\n", indent, Definition.ToString(indent + " "));
            }
            return sb.ToString();
        }
    }
}
