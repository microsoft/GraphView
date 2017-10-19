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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    internal class ToJsonServerStringVisitor : WSqlFragmentVisitor
    {
        private readonly Stack<string> dfsStack;

        public ToJsonServerStringVisitor()
        {
            this.dfsStack = new Stack<string>();
        }

        public void Invoke(WSqlFragment node)
        {
            node.Accept(this);
        }

        public string GetString()
        {
            Debug.Assert(this.dfsStack.Count == 1);
            return this.dfsStack.Pop();
        }

        // WBooleanExpression
        public override void Visit(WBooleanBinaryExpression node)
        {
            node.FirstExpr.Accept(this);
            string left = this.dfsStack.Pop();
            node.SecondExpr.Accept(this);
            string right = this.dfsStack.Pop();

            string nodeStr = $"{left} {TsqlFragmentToString.BooleanExpressionType(node.BooleanExpressionType)} {right}";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WBooleanComparisonExpression node)
        {

            node.FirstExpr.Accept(this);
            string left = this.dfsStack.Pop();
            node.SecondExpr.Accept(this);
            string right = this.dfsStack.Pop();

            // TODO: Consider to ues ISNULLExpression
            string nodeStr = right == "null"
                ? $"{left} IS {right}"
                : $"{left} {TsqlFragmentToString.BooleanComparisonType(node.ComparisonType)} {right}";

            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WBooleanIsNullExpression node)
        {
            node.Expression.Accept(this);
            string expr = this.dfsStack.Pop();

            string nodeStr = expr + (node.IsNot ? " IS NOT NULL" : " IS NULL");
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WBooleanNotExpression node)
        {
            node.Expression.Accept(this);
            string expr = this.dfsStack.Pop();

            string nodeStr = $"NOT {expr}";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WBooleanParenthesisExpression node)
        {
            node.Expression.Accept(this);
            string expr = this.dfsStack.Pop();

            string nodeStr = $"({expr})";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WBetweenExpression node)
        {
            throw new NotImplementedException();
        }

        public override void Visit(WExistsPredicate node)
        {
            throw new NotImplementedException();
        }

        public override void Visit(WInPredicate node)
        {
            Debug.Assert(node.Subquery == null);

            node.Expression.Accept(this);
            string left = this.dfsStack.Pop();
            List<string> values = new List<string>();
            foreach (var wScalarExpression in node.Values)
            {
                var wv = (WValueExpression)wScalarExpression;
                Debug.Assert(wv != null);
                wv.Accept(this);
                values.Add(this.dfsStack.Pop());
            }
            this.dfsStack.Push($"{left} {(node.NotDefined ? "NOT IN" : "IN")} ({string.Join(", ", values)})");
        }

        public override void Visit(WLikePredicate node)
        {
            throw new NotImplementedException();
        }

        public override void Visit(WSubqueryComparisonPredicate node)
        {
            throw new NotImplementedException();
        }

        // WScalarExpression
        public override void Visit(WBinaryExpression node)
        {
            node.FirstExpr.Accept(this);
            string left = this.dfsStack.Pop();
            node.SecondExpr.Accept(this);
            string right = this.dfsStack.Pop();

            string nodeStr = $"{left} {TsqlFragmentToString.BinaryExpressionType(node.ExpressionType)} {right}";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WUnaryExpression node)
        {
            node.Expression.Accept(this);
            string expr = this.dfsStack.Pop();

            string nodeStr = $"{TsqlFragmentToString.UnaryExpressionType(node.ExpressionType)}{expr}";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WEdgeColumnReferenceExpression node)
        {
            string nodeStr = string.IsNullOrEmpty(node.Alias) ? node.MultiPartIdentifier.ToString() : $"{node.MultiPartIdentifier} AS {node.Alias}";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            string nodeStr = node.MultiPartIdentifier != null
                ? node.MultiPartIdentifier.ToString()
                : "";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WFunctionCall node)
        {
            throw new NotImplementedException();
        }

        public override void Visit(WValueExpression node)
        {
            string value = node.ToString("");
            // TODO: double check, how to use boolean type in database?
//            if (value == "true" || value == "false")
//            {
//                value = $"'{value}'";
//            }
            this.dfsStack.Push(value);
        }

        public void Visit(WVariableReference node)
        {
            this.dfsStack.Push(node.Name);
        }

        public override void Visit(WParenthesisExpression node)
        {
            node.Expression.Accept(this);
            string expr = this.dfsStack.Pop();

            string nodeStr = $"({expr})";
            this.dfsStack.Push(nodeStr);
        }

        public override void Visit(WCaseExpression node)
        {
            throw new NotImplementedException();
        }

        public override void Visit(WSearchedCaseExpression node)
        {
            throw new NotImplementedException();
        }

        public override void Visit(WCastCall node)
        {
            throw new NotImplementedException();
        }

        public void Visit(WRepeatConditionExpression node)
        {
            throw new NotImplementedException();
        }

        public void Visit(WColumnNameList node)
        {
            throw new NotImplementedException();
        }

        public void Visit(Identifier node)
        {
            throw new NotImplementedException();
        }
    }
}
