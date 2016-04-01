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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class ConvertToModificationStatementVisitor : WSqlFragmentVisitor
    {
        private IList<GraphDataModificationAnnotation> _annotations;
        private int _nodeCount;

        public void Invoke(WSqlFragment fragment,
            IList<GraphDataModificationAnnotation> annotations)
        {
            _annotations = annotations;
            _nodeCount = 0;
            fragment.Accept(this);
        }

        public override void Visit(WSqlBatch node)
        {
            base.Visit(node);
            ConvertToGraphModificationStatement(node.Statements);
        }

        public override void Visit(WProcedureStatement node)
        {
            base.Visit(node);
            ConvertToGraphModificationStatement(node.StatementList);
        }

        public override void Visit(WBeginEndBlockStatement node)
        {
            base.Visit(node);
            ConvertToGraphModificationStatement(node.StatementList);
        }

        public override void Visit(WIfStatement node)
        {
            base.Visit(node);
         
            var statementList = new List<WSqlStatement>(1);
            statementList.Add(node.ThenStatement);
            ConvertToGraphModificationStatement(statementList);
            node.ThenStatement = statementList[0];

            if (node.ElseStatement != null)
            {
                statementList.Clear();
                statementList.Add(node.ElseStatement);
                ConvertToGraphModificationStatement(statementList);
                node.ElseStatement = statementList[0];
            }
        }

        /// <summary>
        /// convert statements in annotation to according graph data modification statements
        /// </summary>
        /// <param name="script">SQL script</param>
        /// <param name="annotations">stores information of data modification statements</param>
        /// <returns>Converted script</returns>
        private void ConvertToGraphModificationStatement(IList<WSqlStatement> statements)
        {
            for (var j = 0; j < statements.Count; ++j)
            {
                var stmt = statements[j];
                if (_nodeCount >= _annotations.Count)
                    break;
                if (stmt is WSelectStatement && stmt.FirstTokenIndex == _annotations[_nodeCount].Position)
                {
                    var newStmt = (stmt as WSelectStatement).QueryExpr as WSelectQueryBlock;
                    var annotation = _annotations[_nodeCount] as DeleteEdgeAnnotation;
                    if (annotation != null)
                        newStmt.MatchClause = new WMatchClause
                        {
                            Paths = new List<WMatchPath>
                            {
                                annotation.Path
                            }
                        };
                    statements[j] = new WDeleteEdgeSpecification(newStmt);
                    _nodeCount++;
                }
                if (!(stmt is WInsertSpecification) &&
                    !(stmt is WDeleteSpecification))
                    continue;
                if (stmt.LastTokenIndex < _annotations[_nodeCount].Position)
                {
                    continue;
                }
                if (_annotations[_nodeCount] is InsertNodeAnnotation)
                {
                    statements[j] = new WInsertNodeSpecification(stmt as WInsertSpecification);
                }
                else if (_annotations[_nodeCount] is InsertEdgeAnnotation)
                {
                    var annotation = _annotations[_nodeCount] as InsertEdgeAnnotation;
                    statements[j] = new WInsertEdgeSpecification(stmt as WInsertSpecification)
                    {
                        EdgeColumn = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(annotation.EdgeColumn)
                        },
                    };
                }
                else if (_annotations[_nodeCount] is DeleteNodeAnnotation)
                {
                    statements[j] = new WDeleteNodeSpecification(stmt as WDeleteSpecification);
                }
                _nodeCount++;
            }
        }
    }
}
