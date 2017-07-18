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

namespace GraphView
{
    public abstract class WSqlFragmentVisitor
    {
        public virtual void Visit(WSqlFragment fragment)
        {
        }

        public virtual void Visit(WSqlScript node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSqlBatch node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSqlStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WStatementWithCtesAndXmlNamespaces node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBeginEndBlockStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectQueryExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WQueryParenthesisExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBinaryQueryExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectQueryBlock node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WTopRowFilter node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectElement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectScalarExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectStarExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectSetVariable node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WFromClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WWhereClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WOrderByClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WGroupByClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WHavingClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WWhenClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSearchedWhenClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSetClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WAssignmentSetClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WMatchClause node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WMatchPath node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBetweenExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBooleanBinaryExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBooleanComparisonExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBooleanExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBooleanIsNullExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBooleanNotExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBooleanParenthesisExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WExistsPredicate node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WInPredicate node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WLikePredicate node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSubqueryComparisonPredicate node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WBinaryExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WCaseExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WCastCall node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WColumnReferenceExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WEdgeColumnReferenceExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WFunctionCall node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WParenthesisExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WPrimaryExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WScalarExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WScalarSubquery node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSearchedCaseExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WUnaryExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WValueExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WCompositeGroupingSpec node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WExpressionGroupingSpec node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WExpressionWithSortOrder node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WGroupingSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WJoinTableReference node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WNamedTableReference node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WParenthesisTableReference node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WQualifiedJoin node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WQueryDerivedTable node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WTableReference node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WTableReferenceWithAlias node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WTableReferenceWithAliasAndColumns node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WProcedureStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WFunctionStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WCreateFunctionStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WDataModificationSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WUpdateDeleteSpecificationBase node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WInsertSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WInsertNodeSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WInsertEdgeSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WDeleteSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WDeleteNodeSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WDeleteEdgeSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WUpdateSpecification node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WInsertSource node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSelectInsertSource node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WValuesInsertSource node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WRowValue node)
        {
            node.AcceptChildren(this);
        }
        //public virtual void Visit(WCreateProcedureStatement node)
        //{
        //    node.AcceptChildren(this);
        //}
        public virtual void Visit(WWhileStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WIfStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WCommonTableExpression node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WMultiCommonTableExpression node)
        {
            node.AcceptChildren(this);
        }
        public virtual void Visit(WDeclareVariableStatement node)
        {
            node.AcceptChildren(this);
        }
        public virtual void Visit(WCreateViewStatement node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WSchemaObjectFunctionTableReference node)
        {
            node.AcceptChildren(this);
        }

        public virtual void Visit(WVariableTableReference node)
        {
            node.AcceptChildren(this);
        }
    }
}
