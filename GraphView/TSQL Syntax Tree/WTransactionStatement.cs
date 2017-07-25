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
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    public abstract class WTransactionStatement : WSqlStatement
    {
        public WIdentifierOrValueExpression Name { get; set; }
        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
            base.Accept(visitor);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Name != null)
                Name.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public class WBeginTransactionStatement : WTransactionStatement
    {
        public bool Distributed { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}BEGIN ", indent);
            if (Distributed)
                sb.Append("DISTRIBUTED");
            sb.AppendFormat("TRANSACTION {0}\r\n", Name);
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
            base.Accept(visitor);
        }
    }

    public class WCommitTransactionStatement : WTransactionStatement
    {
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}COMMIT TRANSACTION {1}\r\n", indent, Name);
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
            base.Accept(visitor);
        }
    }

    public class WRollbackTransactionStatement : WTransactionStatement
    {
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}ROLLBACK TRANSACTION {1}\r\n", indent, Name);
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
            base.Accept(visitor);
        }
    }

    public class WSaveTransactionStatement : WTransactionStatement
    {
        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}SAVE TRANSACTION {1}\r\n", indent, Name);
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
            base.Accept(visitor);
        }
    }
}
