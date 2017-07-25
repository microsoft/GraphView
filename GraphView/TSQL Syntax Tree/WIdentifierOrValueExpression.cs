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

using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class WIdentifierOrValueExpression : WSqlFragment
    {
        public string Value
        {
            get
            {
                if (Identifier != null)
                {
                    return Identifier.Value;
                }
                return ValueExpression == null ? null : ValueExpression.Value;
            }
        }
        public Identifier Identifier { get; set; }
        public WValueExpression ValueExpression { get; set; }

        internal override bool OneLine()
        {
            return true;
        }

        internal override string ToString(string indent)
        {
            if (Identifier != null)
            {
                return Identifier.ToString(indent);
            }
            else
            {
                return ValueExpression == null ? null : ValueExpression.ToString(indent);
            }
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (ValueExpression != null)
                ValueExpression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }
}
