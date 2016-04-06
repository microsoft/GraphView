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
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// The visitor that puts the MATCH clause back to the SELECT query block
    /// </summary>
    class MatchClauseVisitor : WSqlFragmentVisitor
    {
        /// <summary>
        /// List of MATCH clauses
        /// </summary>
        public IList<WMatchClause> MatchList { get; set; }
        /// <summary>
        /// Label if MATCH clause is visited
        /// </summary>
        public IList<bool> MatchFlag { get; set; }
        public List<TSqlParserToken> Tokens { get; set; }
        private IList<ParseError> _errors; 

        public void Invoke(WSqlFragment node, ref IList<ParseError> errors)
        {
            _errors = errors;
            node.Accept(this);
        }
        public override void Visit(WSelectQueryBlock node)
        {
            base.Visit(node);
            var flag = false;
            for (var i = 0; i < MatchList.Count; ++i)
            {
                if (MatchFlag[i])
                    continue;

                var nextToken = node.LastTokenIndex + 1;
                while (nextToken < Tokens.Count && Tokens[nextToken].TokenType == TSqlTokenType.WhiteSpace)
                    nextToken++;

                var insideBlock = (node.FirstTokenIndex < MatchList[i].FirstTokenIndex &&
                                   (node.LastTokenIndex > MatchList[i].LastTokenIndex ||
                                    nextToken == MatchList[i].FirstTokenIndex));
                if (!insideBlock)
                    continue;

                nextToken = node.FromClause.LastTokenIndex + 1;
                while (nextToken < Tokens.Count && Tokens[nextToken].TokenType == TSqlTokenType.WhiteSpace)
                    nextToken++;
                // match clause should exactly follow FROM clause
                if (node.FromClause.TableReferences == null ||
                    nextToken != MatchList[i].FirstTokenIndex)
                {
                    var token = Tokens[nextToken];
                    _errors.Add(new ParseError(0, token.Offset, token.Line, token.Column,
                        "MATCH clause should exactly follow FROM clause"));
                }

                // if a where/top row filter/group by/having clause exists, it should be followed by a match clause
                if (node.WhereClause.SearchCondition != null &&
                    node.WhereClause.FirstTokenIndex < MatchList[i].LastTokenIndex)
                {
                    var token = Tokens[nextToken];
                    _errors.Add(new ParseError(0, token.Offset, token.Line, token.Column,
                        "WHERE clause should be followed by a MATCH clause"));
                }
                if (node.TopRowFilter != null &&
                    node.TopRowFilter.FirstTokenIndex < MatchList[i].LastTokenIndex)
                {
                    var token = Tokens[nextToken];
                    _errors.Add(new ParseError(0, token.Offset, token.Line, token.Column,
                        "Top row filter should be followed by a MATCH clause"));
                }
                if (node.GroupByClause != null &&
                    node.GroupByClause.FirstTokenIndex < MatchList[i].LastTokenIndex)
                {
                    var token = Tokens[nextToken];
                    _errors.Add(new ParseError(0, token.Offset, token.Line, token.Column,
                        "GROUP BY clause should be followed by a MATCH clause"));
                }
                if (node.HavingClause != null &&
                    node.HavingClause.FirstTokenIndex < MatchList[i].LastTokenIndex)
                {
                    var token = Tokens[nextToken];
                    _errors.Add(new ParseError(0, token.Offset, token.Line, token.Column,
                        "HAVING clause should be followed by a MATCH clause"));
                }
                if (flag)
                {
                    var token = Tokens[nextToken];
                    _errors.Add(new ParseError(0, token.Offset, token.Line, token.Column,
                        "Mutiple MATCH clauses in same query block"));
                }
                if (_errors.Count > 0)
                    return;
                if (node.LastTokenIndex < MatchList[i].FirstTokenIndex)
                {
                    node.LastTokenIndex = MatchList[i].LastTokenIndex;
                }

                flag = true;
                if (node.MatchClause != null && node.MatchClause.Paths != null && node.MatchClause.Paths.Any())
                {
                    foreach (var path in MatchList[i].Paths)
                    {
                        node.MatchClause.Paths.Add(path);
                    }
                }
                else
                {
                    node.MatchClause = MatchList[i];
                }
                MatchFlag[i] = true;
            }

        }
    }
}
