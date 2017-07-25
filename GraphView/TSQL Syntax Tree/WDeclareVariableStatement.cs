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
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class WDeclareVariableStatement:WSqlStatement
    {
        public DeclareVariableStatement Statement{ get; set; }

        internal override string ToString(string indent)
        {
            var _tokenStream = new List<TSqlParserToken>(Statement.LastTokenIndex - Statement.FirstTokenIndex + 1);

            for (var pos = Statement.FirstTokenIndex; pos <= Statement.LastTokenIndex; pos++)
            {
                _tokenStream.Add(Statement.ScriptTokenStream[pos]);
            }
            var newLine = true;
            var sb = new StringBuilder(_tokenStream.Count * 8);
            foreach (var token in _tokenStream)
            {
                if (newLine)
                {
                    sb.Append(indent);
                }
                sb.Append(token.Text);

                newLine = false;

                if (token.TokenType != TSqlTokenType.WhiteSpace)
                    continue;
                if (token.Text.Equals("\r\n") || token.Text.Equals("\n"))
                {
                    newLine = true;
                }
            }
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }
}
