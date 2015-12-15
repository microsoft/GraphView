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
    /// <summary>
    /// Syntax tree node of data type, could be a Parameterized data type or XML data type.
    /// </summary>
    public abstract partial class WDataTypeReference : WSqlFragment
    {
        private WSchemaObjectName _name;
        public WSchemaObjectName Name {
            get { return _name; }
            set { UpdateTokenInfo(value); _name = value; }
        }
    }

    /// <summary>
    /// Syntax tree node of data type with parameters, corresponding to standard SQL data type and User-defined data type.
    /// </summary>
    public partial class WParameterizedDataTypeReference : WDataTypeReference
    {
        public IList<Literal> Parameters { get; set; }

        internal override bool OneLine()
        {
            return true;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(Name);
            if (Parameters == null || Parameters.Count == 0)
                return sb.ToString();
            sb.Append("(");
            for (var i = 0; i < Parameters.Count; ++i)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append(Parameters[i].Value);
            }
            sb.Append(")");
            return sb.ToString();
        }
    }

    /// <summary>
    /// Syntax tree node of XML data type.
    /// </summary>
    public partial class WXmlDataTypeReference : WDataTypeReference
    {
        private WSchemaObjectName _xmlSchemaCollection;

        public WSchemaObjectName XmlSchemaCollection
        {
            get { return _xmlSchemaCollection; }
            set { UpdateTokenInfo(value); _xmlSchemaCollection = value; }
        }

        public XmlDataTypeOption XmlDataTypeOption { get; set; }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}{1}(", indent, Name);
            if (XmlDataTypeOption != XmlDataTypeOption.None)
                sb.AppendFormat("{0} ", XmlDataTypeOption);
            sb.AppendFormat("{0})", XmlSchemaCollection);
            return sb.ToString();
        }
    }

}