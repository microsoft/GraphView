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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class WSqlParser
    {
        internal TSql120Parser tsqlParser;
        private IList<TSqlParserToken> _tokens;

        public WSqlParser()
        {
            tsqlParser = new TSql120Parser(true);
        }

        public WSqlFragment Parse(IList<TSqlParserToken> tokens, out IList<ParseError> errors)
        {
            var fragment = tsqlParser.Parse(tokens, out errors);
            if (errors.Count > 0)
            {
                return null;
            }
            _tokens = tokens;
            return ConvertFragment(fragment);

        }

        public WSqlFragment Parse(TextReader queryInput, out IList<ParseError> errors)
        {
            var fragment = tsqlParser.Parse(queryInput, out errors);
            if (errors.Count > 0)
            {
                return null;
            }

            return ConvertFragment(fragment);
        }

        public WBooleanExpression ParseWhereClauseFromSelect(string selectQuery)
        {
            IList<ParseError> errors;
            TSqlFragment fragment = tsqlParser.Parse(new StringReader(selectQuery), out errors);

            if (errors.Count > 0)
            {
                throw new Exception();
            }

            SelectVisitor visitor = new SelectVisitor();
            fragment.Accept(visitor);

            SelectStatement statement = visitor.SelectStatements.FirstOrDefault();
            QuerySpecification specification = statement.QueryExpression as QuerySpecification;

            return ParseBooleanExpression(specification.WhereClause.SearchCondition);
        }

        private WSqlFragment ConvertFragment(TSqlFragment fragment)
        {

            var tscript = fragment as TSqlScript;

            var wscript = new WSqlScript
            {
                FirstTokenIndex = tscript.FirstTokenIndex,
                LastTokenIndex = tscript.LastTokenIndex,
                Batches = tscript.Batches == null ? null : new List<WSqlBatch>(tscript.Batches.Count),
            };

            foreach (var tbatch in tscript.Batches)
            {
                var wbatch = new WSqlBatch
                {
                    FirstTokenIndex = tbatch.FirstTokenIndex,
                    LastTokenIndex = tbatch.LastTokenIndex,
                    Statements = new List<WSqlStatement>(tbatch.Statements.Count),
                };

                foreach (var wstat in tbatch.Statements.Select(ParseStatement))
                {
                    wbatch.Statements.Add(wstat);
                }

                wscript.Batches.Add(wbatch);
            }

            return wscript;
        }

        private WSqlStatement ParseStatement(TSqlStatement tsqlStat)
        {
            if (tsqlStat == null)
            {
                return null;
            }

            WSqlStatement wstat;

            switch (tsqlStat.GetType().Name)
            {
                case "SelectStatement":
                    {
                        var sel = tsqlStat as SelectStatement;
                        WSelectStatement wselstat = new WSelectStatement
                        {
                            FirstTokenIndex = sel.FirstTokenIndex,
                            LastTokenIndex = sel.LastTokenIndex,
                            Into = ParseSchemaObjectName(sel.Into),
                            OptimizerHints = sel.OptimizerHints,
                            QueryExpr = ParseSelectQueryStatement(sel.QueryExpression)

                        };
                        wstat = wselstat;

                        break;
                    }
                default:
                    {
                        throw new NotImplementedException();
                    }
            }

            return wstat;
        }

        private WDataTypeReference ParseDataType(DataTypeReference dataType)
        {
            if (dataType == null)
                return null;
            var pDataType = dataType as ParameterizedDataTypeReference;
            if (pDataType == null)
                throw new NotImplementedException();
            var wDataType = new WParameterizedDataTypeReference
            {
                Name = ParseSchemaObjectName(pDataType.Name),
                FirstTokenIndex = pDataType.FirstTokenIndex,
                LastTokenIndex = pDataType.LastTokenIndex
            };
            if (pDataType.Parameters == null)
                return wDataType;
            wDataType.Parameters = new List<Literal>(pDataType.Parameters.Count);
            foreach (var param in pDataType.Parameters)
                wDataType.Parameters.Add(param);
            return wDataType;
        }

        private WSelectQueryExpression ParseSelectQueryStatement(QueryExpression queryExpr)
        {

            if (queryExpr == null)
            {
                return null;
            }

            switch (queryExpr.GetType().Name)
            {
                case "BinaryQueryExpression":
                    {
                        var bqe = queryExpr as BinaryQueryExpression;
                        var pQueryExpr = new WBinaryQueryExpression
                        {
                            All = bqe.All,
                            FirstQueryExpr = ParseSelectQueryStatement(bqe.FirstQueryExpression),
                            SecondQueryExpr = ParseSelectQueryStatement(bqe.SecondQueryExpression),
                            FirstTokenIndex = bqe.FirstTokenIndex,
                            LastTokenIndex = bqe.LastTokenIndex
                        };

                        //pQueryExpr.OrderByExpr = parseOrderbyExpr(bqe.OrderByClause);

                        return pQueryExpr;
                    }
                case "QueryParenthesisExpression":
                    {
                        var qpe = queryExpr as QueryParenthesisExpression;
                        var pQueryExpr = new WQueryParenthesisExpression
                        {
                            QueryExpr = ParseSelectQueryStatement(qpe.QueryExpression),
                            FirstTokenIndex = qpe.FirstTokenIndex,
                            LastTokenIndex = qpe.LastTokenIndex
                        };

                        //pQueryExpr.OrderByExpr = parseOrderbyExpr(qpe.OrderByClause);

                        return pQueryExpr;
                    }
                case "QuerySpecification":
                    {
                        var qs = queryExpr as QuerySpecification;
                        var pQueryExpr = new WSelectQueryBlock
                        {
                            FirstTokenIndex = qs.FirstTokenIndex,
                            LastTokenIndex = qs.LastTokenIndex,
                            SelectElements = new List<WSelectElement>(qs.SelectElements.Count),
                        };

                        //
                        // SELECT clause
                        // 
                        foreach (var wsel in qs.SelectElements.Select(ParseSelectElement).Where(wsel => wsel != null))
                        {
                            pQueryExpr.SelectElements.Add(wsel);
                        }

                        //
                        // Top row filter
                        // 
                        if (qs.TopRowFilter != null)
                        {
                            pQueryExpr.TopRowFilter = new WTopRowFilter
                            {
                                Percent = qs.TopRowFilter.Percent,
                                WithTies = qs.TopRowFilter.WithTies,
                                Expression = ParseScalarExpression(qs.TopRowFilter.Expression),
                                FirstTokenIndex = qs.TopRowFilter.FirstTokenIndex,
                                LastTokenIndex = qs.TopRowFilter.LastTokenIndex
                            };
                        }

                        pQueryExpr.UniqueRowFilter = ConvertFromScriptDom(qs.UniqueRowFilter);

                        //
                        // FROM clause
                        //
                        if (qs.FromClause != null && qs.FromClause.TableReferences != null)
                        {
                            pQueryExpr.FromClause.FirstTokenIndex = qs.FromClause.FirstTokenIndex;
                            pQueryExpr.FromClause.LastTokenIndex = qs.FromClause.LastTokenIndex;
                            pQueryExpr.FromClause.TableReferences = new List<WTableReference>(qs.FromClause.TableReferences.Count);
                            foreach (var pref in qs.FromClause.TableReferences.Select(ParseTableReference).Where(pref => pref != null))
                            {
                                pQueryExpr.FromClause.TableReferences.Add(pref);
                            }
                        }

                        //
                        // WHERE clause
                        //

                        if (qs.WhereClause != null && qs.WhereClause.SearchCondition != null)
                        {
                            pQueryExpr.WhereClause.FirstTokenIndex = qs.WhereClause.FirstTokenIndex;
                            pQueryExpr.WhereClause.LastTokenIndex = qs.WhereClause.LastTokenIndex;
                            pQueryExpr.WhereClause.SearchCondition = ParseBooleanExpression(qs.WhereClause.SearchCondition);
                        }

                        // GROUP-BY clause
                        if (qs.GroupByClause != null)
                        {
                            pQueryExpr.GroupByClause = ParseGroupbyClause(qs.GroupByClause);
                        }

                        // Having clause
                        if (qs.HavingClause != null)
                        {
                            pQueryExpr.HavingClause = new WHavingClause
                            {
                                SearchCondition = ParseBooleanExpression(qs.HavingClause.SearchCondition),
                                FirstTokenIndex = qs.HavingClause.FirstTokenIndex,
                                LastTokenIndex = qs.HavingClause.LastTokenIndex
                            };
                        }

                        //
                        // ORDER-BY clause
                        // 
                        if (qs.OrderByClause != null)
                        {
                            pQueryExpr.OrderByClause = ParseOrderbyClause(qs.OrderByClause);
                        }

                        return pQueryExpr;
                    }
                default:
                    return null;
            }
        }

        private WSelectElement ParseSelectElement(SelectElement sel)
        {
            if (sel == null)
            {
                return null;
            }

            switch (sel.GetType().Name)
            {
                case "SelectScalarExpression":
                    {
                        var sse = sel as SelectScalarExpression;
                        var pScalarExpr = new WSelectScalarExpression
                        {
                            SelectExpr = ParseScalarExpression(sse.Expression),
                            FirstTokenIndex = sse.FirstTokenIndex,
                            LastTokenIndex = sse.LastTokenIndex
                        };
                        if (sse.ColumnName != null)
                        {
                            pScalarExpr.ColumnName = sse.ColumnName.Value;
                        }

                        return pScalarExpr;
                    }
                case "SelectStarExpression":
                    {
                        var sse = sel as SelectStarExpression;
                        return new WSelectStarExpression()
                        {
                            FirstTokenIndex = sse.FirstTokenIndex,
                            LastTokenIndex = sse.LastTokenIndex,
                            Qulifier = ParseMultiPartIdentifier(sse.Qualifier)
                        };
                    }
                case "SelectSetVariable":
                    {
                        var ssv = sel as SelectSetVariable;
                        return new WSelectSetVariable
                        {
                            VariableName = ssv.Variable.Name,
                            Expression = ParseScalarExpression(ssv.Expression),
                            AssignmentType = ConvertFromScriptDom(ssv.AssignmentKind),
                            FirstTokenIndex = ssv.FirstTokenIndex,
                            LastTokenIndex = ssv.LastTokenIndex
                        };
                    }
                default:
                    {
                        return null;
                    }
            }
        }

        private WScalarExpression ParseScalarExpression(ScalarExpression scalarExpr)
        {
            if (scalarExpr == null)
            {
                return null;
            }

            switch (scalarExpr.GetType().Name)
            {
                case "BinaryExpression":
                    {
                        var bexpr = scalarExpr as BinaryExpression;
                        var wexpr = new WBinaryExpression
                        {
                            ExpressionType = ConvertFromScriptDom(bexpr.BinaryExpressionType),
                            FirstExpr = ParseScalarExpression(bexpr.FirstExpression),
                            SecondExpr = ParseScalarExpression(bexpr.SecondExpression),
                            FirstTokenIndex = bexpr.FirstTokenIndex,
                            LastTokenIndex = bexpr.LastTokenIndex,
                        };

                        return wexpr;
                    }
                case "UnaryExpression":
                    {
                        var uexpr = scalarExpr as UnaryExpression;
                        var wuexpr = new WUnaryExpression
                        {
                            Expression = ParseScalarExpression(uexpr.Expression),
                            ExpressionType = ConvertFromScriptDom(uexpr.UnaryExpressionType),
                            FirstTokenIndex = uexpr.FirstTokenIndex,
                            LastTokenIndex = uexpr.LastTokenIndex
                        };

                        return wuexpr;
                    }
                case "ColumnReferenceExpression":
                    {
                        var cre = scalarExpr as ColumnReferenceExpression;

                        if (cre.MultiPartIdentifier.Count == 1)
                        {
                            var wexpr = new WValueExpression
                            {
                                FirstTokenIndex = scalarExpr.FirstTokenIndex,
                                LastTokenIndex = scalarExpr.LastTokenIndex,
                            };
                            wexpr.Value = cre.MultiPartIdentifier.Identifiers[0].Value;
                            Debug.Assert(cre.MultiPartIdentifier.Identifiers[0].QuoteType != Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.SquareBracket);
                            wexpr.SingleQuoted = cre.MultiPartIdentifier.Identifiers[0].QuoteType != Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.NotQuoted;
                            return wexpr;
                        }

                        var wcolRefExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = ParseMultiPartIdentifier(cre.MultiPartIdentifier),
                            ColumnType = ConvertFromScriptDom(cre.ColumnType),
                            FirstTokenIndex = cre.FirstTokenIndex,
                            LastTokenIndex = cre.LastTokenIndex
                        };

                        return wcolRefExpr;
                    }
                case "ScalarSubquery":
                    {
                        var oquery = scalarExpr as ScalarSubquery;
                        var wexpr = new WScalarSubquery
                        {
                            SubQueryExpr = ParseSelectQueryStatement(oquery.QueryExpression),
                            FirstTokenIndex = oquery.FirstTokenIndex,
                            LastTokenIndex = oquery.LastTokenIndex
                        };

                        return wexpr;
                    }
                case "ParenthesisExpression":
                    {
                        var parenExpr = scalarExpr as ParenthesisExpression;
                        var wexpr = new WParenthesisExpression
                        {
                            Expression = ParseScalarExpression(parenExpr.Expression),
                            FirstTokenIndex = parenExpr.FirstTokenIndex,
                            LastTokenIndex = parenExpr.LastTokenIndex,
                        };

                        return wexpr;
                    }
                case "FunctionCall":
                    {
                        var fc = scalarExpr as FunctionCall;
                        var wexpr = new WFunctionCall
                        {
                            CallTarget = ParseCallTarget(fc.CallTarget),
                            FunctionName = ConvertFromScriptDom(fc.FunctionName),
                            UniqueRowFilter = ConvertFromScriptDom(fc.UniqueRowFilter),
                            FirstTokenIndex = fc.FirstTokenIndex,
                            LastTokenIndex = fc.LastTokenIndex,
                        };

                        if (fc.Parameters == null) return wexpr;
                        wexpr.Parameters = new List<WScalarExpression>(fc.Parameters.Count);
                        foreach (var pe in fc.Parameters.Select(ParseScalarExpression).Where(pe => pe != null))
                        {
                            wexpr.Parameters.Add(pe);
                        }

                        return wexpr;
                    }
                case "SearchedCaseExpression":
                    {
                        var caseExpr = scalarExpr as SearchedCaseExpression;
                        var wexpr = new WSearchedCaseExpression
                        {
                            FirstTokenIndex = caseExpr.FirstTokenIndex,
                            LastTokenIndex = caseExpr.LastTokenIndex,
                            WhenClauses = new List<WSearchedWhenClause>(caseExpr.WhenClauses.Count)
                        };

                        foreach (var pwhen in caseExpr.WhenClauses.Select(swhen => new WSearchedWhenClause
                        {
                            WhenExpression = ParseBooleanExpression(swhen.WhenExpression),
                            ThenExpression = ParseScalarExpression(swhen.ThenExpression),
                            FirstTokenIndex = swhen.FirstTokenIndex,
                            LastTokenIndex = swhen.LastTokenIndex,
                        }))
                        {
                            wexpr.WhenClauses.Add(pwhen);
                        }

                        wexpr.ElseExpr = ParseScalarExpression(caseExpr.ElseExpression);

                        return wexpr;
                    }
                case "CastCall":
                    {
                        var castExpr = scalarExpr as CastCall;
                        var wexpr = new WCastCall
                        {
                            DataType = ParseDataType(castExpr.DataType),
                            Parameter = ParseScalarExpression(castExpr.Parameter),
                            FirstTokenIndex = castExpr.FirstTokenIndex,
                            LastTokenIndex = castExpr.LastTokenIndex,
                        };

                        return wexpr;
                    }
                default:
                    {
                        if (!(scalarExpr is ValueExpression)) return null;
                        var wexpr = new WValueExpression
                        {
                            FirstTokenIndex = scalarExpr.FirstTokenIndex,
                            LastTokenIndex = scalarExpr.LastTokenIndex,
                        };

                        var expr = scalarExpr as Literal;
                        if (expr != null)
                        {
                            wexpr.Value = expr.Value;

                            if (expr.LiteralType == LiteralType.String)
                            {
                                wexpr.SingleQuoted = true;
                            }
                        }
                        else
                        {
                            var reference = scalarExpr as VariableReference;
                            wexpr.Value = reference != null ? reference.Name : ((GlobalVariableExpression)scalarExpr).Name;
                        }

                        return wexpr;
                    }
            }
        }

        private WTableReference ParseTableReference(TableReference tabRef)
        {
            if (tabRef == null)
            {
                return null;
            }
            var tabRefWithAlias = tabRef as TableReferenceWithAlias;
            if (tabRefWithAlias != null && tabRefWithAlias.Alias != null &&
                DocumentDBKeywords._keywords.Contains(tabRefWithAlias.Alias.Value))
            {
                var token = _tokens[tabRefWithAlias.Alias.FirstTokenIndex];
                throw new SyntaxErrorException(token.Line, tabRefWithAlias.Alias.Value,
                    "System restricted Name cannot be used");
            }
            switch (tabRef.GetType().Name)
            {
                case "NamedTableReference":
                    {
                        var oref = tabRef as NamedTableReference;
                        if (oref.SchemaObject.BaseIdentifier.QuoteType == Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.NotQuoted &&
                            (oref.SchemaObject.BaseIdentifier.Value[0] == '@' ||
                             oref.SchemaObject.BaseIdentifier.Value[0] == '#'))
                        {
                            var pref = new WSpecialNamedTableReference
                            {
                                Alias = ConvertFromScriptDom(oref.Alias),
                                TableHints = new List<WTableHint>(),
                                FirstTokenIndex = oref.FirstTokenIndex,
                                LastTokenIndex = oref.LastTokenIndex,
                                TableObjectName = ParseSchemaObjectName(oref.SchemaObject),
                            };

                            if (oref.TableHints != null)
                            {
                                foreach (var hint in oref.TableHints)
                                    pref.TableHints.Add(ParseTableHint(hint));
                            }

                            return pref;
                        }
                        else
                        {
                            var pref = new WNamedTableReference
                            {
                                Alias = ConvertFromScriptDom(oref.Alias),
                                TableHints = new List<WTableHint>(),
                                FirstTokenIndex = oref.FirstTokenIndex,
                                LastTokenIndex = oref.LastTokenIndex,
                                TableObjectName = ParseSchemaObjectName(oref.SchemaObject),
                            };

                            if (oref.TableHints != null)
                            {
                                foreach (var hint in oref.TableHints)
                                    pref.TableHints.Add(ParseTableHint(hint));
                            }

                            return pref;
                        }
                    }
                case "QueryDerivedTable":
                    {
                        var oref = tabRef as QueryDerivedTable;
                        var pref = new WQueryDerivedTable
                        {
                            QueryExpr = ParseSelectQueryStatement(oref.QueryExpression),
                            Alias = ConvertFromScriptDom(oref.Alias),
                            Columns = ConvertFromScriptDom(oref.Columns),
                            FirstTokenIndex = oref.FirstTokenIndex,
                            LastTokenIndex = oref.LastTokenIndex,
                        };

                        return pref;
                    }
                case "SchemaObjectFunctionTableReference":
                    {
                        var oref = tabRef as SchemaObjectFunctionTableReference;
                        var pref = new WSchemaObjectFunctionTableReference
                        {
                            Alias = ConvertFromScriptDom(oref.Alias),
                            Columns = ConvertFromScriptDom(oref.Columns),
                            SchemaObject = ParseSchemaObjectName(oref.SchemaObject),
                            FirstTokenIndex = oref.FirstTokenIndex,
                            LastTokenIndex = oref.LastTokenIndex
                        };
                        if (oref.Parameters == null)
                            return pref;
                        pref.Parameters = new List<WScalarExpression>();
                        foreach (var param in oref.Parameters)
                            pref.Parameters.Add(ParseScalarExpression(param));
                        return pref;
                    }
                case "QualifiedJoin":
                    {
                        var oref = tabRef as QualifiedJoin;
                        var pref = new WQualifiedJoin
                        {
                            FirstTableRef = ParseTableReference(oref.FirstTableReference),
                            SecondTableRef = ParseTableReference(oref.SecondTableReference),
                            QualifiedJoinType = ConvertFromScriptDom(oref.QualifiedJoinType),
                            JoinHint = ConvertFromScriptDom(oref.JoinHint),
                            JoinCondition = ParseBooleanExpression(oref.SearchCondition),
                            FirstTokenIndex = oref.FirstTokenIndex,
                            LastTokenIndex = oref.LastTokenIndex,
                        };

                        return pref;
                    }
                case "UnqualifiedJoin":
                    {
                        var oref = tabRef as UnqualifiedJoin;
                        var pref = new WUnqualifiedJoin
                        {
                            FirstTableRef = ParseTableReference(oref.FirstTableReference),
                            SecondTableRef = ParseTableReference(oref.SecondTableReference),
                            UnqualifiedJoinType = ConvertFromScriptDom(oref.UnqualifiedJoinType),
                            FirstTokenIndex = oref.FirstTokenIndex,
                            LastTokenIndex = oref.LastTokenIndex,
                        };
                        return pref;
                    }
                case "JoinParenthesisTableReference":
                    {
                        var ptab = tabRef as JoinParenthesisTableReference;

                        var wptab = new WParenthesisTableReference
                        {
                            Table = ParseTableReference(ptab.Join),
                            FirstTokenIndex = ptab.FirstTokenIndex,
                            LastTokenIndex = ptab.LastTokenIndex,
                        };

                        return wptab;
                    }
                default:
                    return null;
            }
        }

        private WOrderByClause ParseOrderbyClause(OrderByClause orderbyExpr)
        {
            var wobc = new WOrderByClause
            {
                FirstTokenIndex = orderbyExpr.FirstTokenIndex,
                LastTokenIndex = orderbyExpr.LastTokenIndex,
                OrderByElements = new List<WExpressionWithSortOrder>(orderbyExpr.OrderByElements.Count)
            };

            foreach (var pexp in from e in orderbyExpr.OrderByElements
                                 let pscalar = ParseScalarExpression(e.Expression)
                                 where pscalar != null
                                 select new WExpressionWithSortOrder
                                 {
                                     ScalarExpr = pscalar,
                                     SortOrder = ConvertFromScriptDom(e.SortOrder),
                                     FirstTokenIndex = e.FirstTokenIndex,
                                     LastTokenIndex = e.LastTokenIndex
                                 })
            {
                wobc.OrderByElements.Add(pexp);
            }

            return wobc;
        }

        private WGroupByClause ParseGroupbyClause(GroupByClause groupbyExpr)
        {
            if (groupbyExpr == null)
            {
                return null;
            }

            var wgc = new WGroupByClause
            {
                FirstTokenIndex = groupbyExpr.FirstTokenIndex,
                LastTokenIndex = groupbyExpr.LastTokenIndex,
                GroupingSpecifications = new List<WGroupingSpecification>(groupbyExpr.GroupingSpecifications.Count)
            };

            foreach (var gs in groupbyExpr.GroupingSpecifications)
            {
                //if (!(gs is ExpressionGroupingSpecification))
                //    continue;
                var egs = gs as ExpressionGroupingSpecification;
                if (egs == null) continue;
                var pspec = new WExpressionGroupingSpec
                {
                    Expression = ParseScalarExpression(egs.Expression),
                    FirstTokenIndex = egs.FirstTokenIndex,
                    LastTokenIndex = egs.LastTokenIndex,
                };

                wgc.GroupingSpecifications.Add(pspec);
            }

            return wgc;
        }

        private WBooleanExpression ParseBooleanExpression(BooleanExpression bexpr)
        {
            if (bexpr == null)
            {
                return null;
            }

            switch (bexpr.GetType().Name)
            {
                case "BooleanBinaryExpression":
                    {
                        var oexpr = bexpr as BooleanBinaryExpression;
                        var pexpr = new WBooleanBinaryExpression
                        {
                            FirstExpr = ParseBooleanExpression(oexpr.FirstExpression),
                            SecondExpr = ParseBooleanExpression(oexpr.SecondExpression),
                            BooleanExpressionType = ConvertFromScriptDom(oexpr.BinaryExpressionType),
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        return pexpr;
                    }
                case "BooleanComparisonExpression":
                    {
                        var oexpr = bexpr as BooleanComparisonExpression;
                        var pexpr = new WBooleanComparisonExpression
                        {
                            ComparisonType = ConvertFromScriptDom(oexpr.ComparisonType),
                            FirstExpr = ParseScalarExpression(oexpr.FirstExpression),
                            SecondExpr = ParseScalarExpression(oexpr.SecondExpression),
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        return pexpr;
                    }
                case "BooleanIsNullExpression":
                    {
                        var oexpr = bexpr as BooleanIsNullExpression;
                        var pexpr = new WBooleanIsNullExpression
                        {
                            IsNot = oexpr.IsNot,
                            Expression = ParseScalarExpression(oexpr.Expression),
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        return pexpr;
                    }
                case "BooleanNotExpression":
                    {
                        var oexpr = bexpr as BooleanNotExpression;
                        var pexpr = new WBooleanNotExpression
                        {
                            Expression = ParseBooleanExpression(oexpr.Expression),
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        return pexpr;
                    }
                case "BooleanParenthesisExpression":
                    {
                        var oexpr = bexpr as BooleanParenthesisExpression;
                        var pexpr = new WBooleanParenthesisExpression
                        {
                            Expression = ParseBooleanExpression(oexpr.Expression),
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        return pexpr;
                    }
                case "BooleanTernaryExpression":
                    {
                        var oexpr = bexpr as BooleanTernaryExpression;
                        var pexpr = new WBetweenExpression
                        {
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        switch (oexpr.TernaryExpressionType)
                        {
                            case BooleanTernaryExpressionType.Between:
                                pexpr.NotDefined = false;
                                break;
                            case BooleanTernaryExpressionType.NotBetween:
                                pexpr.NotDefined = true;
                                break;
                            default:
                                throw new GraphViewException("Undefined tenary expression type");
                        }

                        pexpr.FirstExpr = ParseScalarExpression(oexpr.FirstExpression);
                        pexpr.SecondExpr = ParseScalarExpression(oexpr.SecondExpression);
                        pexpr.ThirdExpr = ParseScalarExpression(oexpr.ThirdExpression);

                        return pexpr;
                    }
                case "ExistsPredicate":
                    {
                        var oexpr = bexpr as ExistsPredicate;
                        var pexpr = new WExistsPredicate
                        {
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                            Subquery =
                                new WScalarSubquery
                                {
                                    SubQueryExpr = ParseSelectQueryStatement(oexpr.Subquery.QueryExpression),
                                    FirstTokenIndex = oexpr.Subquery.FirstTokenIndex,
                                    LastTokenIndex = oexpr.Subquery.LastTokenIndex,
                                }
                        };

                        return pexpr;
                    }
                case "InPredicate":
                    {
                        var oexpr = bexpr as InPredicate;
                        var pexpr = new WInPredicate
                        {
                            Expression = ParseScalarExpression(oexpr.Expression),
                            NotDefined = oexpr.NotDefined,
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        if (oexpr.Subquery != null)
                        {
                            pexpr.Subquery = new WScalarSubquery
                            {
                                SubQueryExpr = ParseSelectQueryStatement(oexpr.Subquery.QueryExpression),
                                FirstTokenIndex = oexpr.Subquery.FirstTokenIndex,
                                LastTokenIndex = oexpr.Subquery.LastTokenIndex,

                            };
                        }
                        else
                        {
                            pexpr.Values = new List<WScalarExpression>(oexpr.Values.Count);
                            foreach (var wexp in oexpr.Values.Select(ParseScalarExpression).Where(wexp => wexp != null))
                            {
                                pexpr.Values.Add(wexp);
                            }
                        }

                        return pexpr;
                    }
                case "LikePredicate":
                    {
                        var oexpr = bexpr as LikePredicate;
                        var pexpr = new WLikePredicate
                        {
                            EscapeExpr = ParseScalarExpression(oexpr.EscapeExpression),
                            FirstExpr = ParseScalarExpression(oexpr.FirstExpression),
                            SecondExpr = ParseScalarExpression(oexpr.SecondExpression),
                            NotDefined = oexpr.NotDefined,
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                        };

                        return pexpr;
                    }
                case "SubqueryComparisonPredicate":
                    {
                        var oexpr = bexpr as SubqueryComparisonPredicate;
                        var pexpr = new WSubqueryComparisonPredicate
                        {
                            FirstTokenIndex = oexpr.FirstTokenIndex,
                            LastTokenIndex = oexpr.LastTokenIndex,
                            Subquery = new WScalarSubquery()
                            {
                                SubQueryExpr = ParseSelectQueryStatement(oexpr.Subquery.QueryExpression),
                                FirstTokenIndex = oexpr.Subquery.FirstTokenIndex,
                                LastTokenIndex = oexpr.Subquery.LastTokenIndex

                            },
                            ComparisonType = ConvertFromScriptDom(oexpr.ComparisonType),
                            Expression = ParseScalarExpression(oexpr.Expression),
                            SubqueryComparisonType = ConvertFromScriptDom(oexpr.SubqueryComparisonPredicateType),
                        };

                        return pexpr;
                    }
                default:
                    {
                        return null;
                    }
            }
        }

        private WTableHint ParseTableHint(TableHint hint)
        {
            return new WTableHint
            {
                FirstTokenIndex = hint.FirstTokenIndex,
                LastTokenIndex = hint.LastTokenIndex,
                HintKind = hint.HintKind
            };
        }

        private WMultiPartIdentifier ParseMultiPartIdentifier(MultiPartIdentifier name)
        {
            if (name == null)
                return null;
            var wMultiPartIdentifier = new WMultiPartIdentifier
            {
                FirstTokenIndex = name.FirstTokenIndex,
                LastTokenIndex = name.LastTokenIndex
            };
            if (name.Identifiers != null)
            {
                wMultiPartIdentifier.Identifiers = new List<Identifier>();
                for (int i = 0; i < name.Identifiers.Count; ++i)
                {
                    if (DocumentDBKeywords._keywords.Contains(name.Identifiers[i].Value))
                    {
                        var token = _tokens[name.Identifiers[i].FirstTokenIndex];
                        throw new SyntaxErrorException(token.Line, name.Identifiers[i].Value,
                            "System restricted Name cannot be used");
                    }

                    Identifier identifier = new Identifier
                    {
                        FirstTokenIndex = name.Identifiers[i].FirstTokenIndex,
                        LastTokenIndex = name.Identifiers[i].LastTokenIndex,
                        Value = name.Identifiers[i].Value,
                        QuoteType = QuoteType.NotQuoted
                    };

                    if (identifier.Value.Length > 2)
                    {
                        if (identifier.Value.First() == '[' && identifier.Value.Last() == ']')
                        {
                            identifier.QuoteType = QuoteType.SquareBracket;
                        }
                        else if (identifier.Value.First() == '"' && identifier.Value.Last() == '"')
                        {
                            identifier.QuoteType = QuoteType.DoubleQuote;
                        }
                        else if (identifier.Value.First() == '\'' && identifier.Value.Last() == '\'')
                        {
                            identifier.QuoteType = QuoteType.DoubleQuote;
                        }
                    }

                    wMultiPartIdentifier.Identifiers.Add(identifier);
                }
            }
            return wMultiPartIdentifier;
        }

        private WSchemaObjectName ParseSchemaObjectName(SchemaObjectName name)
        {
            if (name == null)
                return null;
            var wSchemaObjectName = new WSchemaObjectName
            {
                FirstTokenIndex = name.FirstTokenIndex,
                LastTokenIndex = name.LastTokenIndex,
            };
            if (name.Identifiers != null)
            {
                wSchemaObjectName.Identifiers = new List<Identifier>();
                foreach (var identifier in name.Identifiers)
                {
                    if (DocumentDBKeywords._keywords.Contains(identifier.Value))
                    {
                        var token = _tokens[identifier.FirstTokenIndex];
                        throw new SyntaxErrorException(token.Line, identifier.Value,
                            "System restricted Name cannot be used");
                    }
                    wSchemaObjectName.Identifiers.Add(ConvertFromScriptDom(identifier));
                }
            }
            return wSchemaObjectName;
        }

        private WCallTarget ParseCallTarget(CallTarget callTarget)
        {
            if (callTarget == null)
                return null;
            WCallTarget result;
            var tCallTarget = callTarget as MultiPartIdentifierCallTarget;
            if (tCallTarget != null)
            {
                result = new WMultiPartIdentifierCallTarget
                {
                    Identifiers = ParseMultiPartIdentifier(tCallTarget.MultiPartIdentifier)
                };
            }
            else
            {
                throw new NotImplementedException();
            }

            return result;
        }

        private UniqueRowFilter ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.UniqueRowFilter uniqueRowFilter)
        {
            switch (uniqueRowFilter)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.UniqueRowFilter.All:
                    return UniqueRowFilter.All;
                case Microsoft.SqlServer.TransactSql.ScriptDom.UniqueRowFilter.Distinct:
                    return UniqueRowFilter.Distinct;
                case Microsoft.SqlServer.TransactSql.ScriptDom.UniqueRowFilter.NotSpecified:
                    return UniqueRowFilter.NotSpecified;
                default:
                    throw new NotImplementedException();
            }
        }

        private AssignmentKind ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind assignmentKind)
        {
            switch (assignmentKind)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.AddEquals:
                    return AssignmentKind.AddEquals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.BitwiseAndEquals:
                    return AssignmentKind.BitwiseAndEquals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.BitwiseOrEquals:
                    return AssignmentKind.BitwiseOrEquals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.BitwiseXorEquals:
                    return AssignmentKind.BitwiseXorEquals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.DivideEquals:
                    return AssignmentKind.DivideEquals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.Equals:
                    return AssignmentKind.Equals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.ModEquals:
                    return AssignmentKind.ModEquals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.MultiplyEquals:
                    return AssignmentKind.MultiplyEquals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.AssignmentKind.SubtractEquals:
                    return AssignmentKind.SubtractEquals;
                default:
                    throw new NotImplementedException();
            }
        }

        private BinaryExpressionType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType binaryExpressionType)
        {
            switch (binaryExpressionType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.Add:
                    return BinaryExpressionType.Add;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.BitwiseAnd:
                    return BinaryExpressionType.BitwiseAnd;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.BitwiseOr:
                    return BinaryExpressionType.BitwiseOr;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.BitwiseXor:
                    return BinaryExpressionType.BitwiseXor;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.Divide:
                    return BinaryExpressionType.Divide;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.Modulo:
                    return BinaryExpressionType.Modulo;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.Multiply:
                    return BinaryExpressionType.Multiply;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpressionType.Subtract:
                    return BinaryExpressionType.Subtract;
                default:
                    throw new NotImplementedException();
            }
        }

        private UnaryExpressionType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpressionType unaryExpressionType)
        {
            switch (unaryExpressionType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpressionType.BitwiseNot:
                    return UnaryExpressionType.BitwiseNot;
                case Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpressionType.Negative:
                    return UnaryExpressionType.Negative;
                case Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpressionType.Positive:
                    return UnaryExpressionType.Positive;
                default:
                    throw new NotImplementedException();
            }
        }

        private ColumnType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType columnType)
        {
            switch (columnType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.IdentityCol:
                    return ColumnType.IdentityCol;
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.PseudoColumnAction:
                    return ColumnType.PseudoColumnAction;
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.PseudoColumnCuid:
                    return ColumnType.PseudoColumnCuid;
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.PseudoColumnIdentity:
                    return ColumnType.PseudoColumnIdentity;
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.PseudoColumnRowGuid:
                    return ColumnType.PseudoColumnRowGuid;
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.Regular:
                    return ColumnType.Regular;
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.RowGuidCol:
                    return ColumnType.RowGuidCol;
                case Microsoft.SqlServer.TransactSql.ScriptDom.ColumnType.Wildcard:
                    return ColumnType.Wildcard;
                default:
                    throw new NotImplementedException();
            }
        }

        private QuoteType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType quoteType)
        {
            switch (quoteType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.DoubleQuote:
                    return QuoteType.DoubleQuote;
                case Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.NotQuoted:
                    return QuoteType.NotQuoted;
                case Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.SquareBracket:
                    return QuoteType.SquareBracket;
                default:
                    throw new NotImplementedException();
            }
        }

        private Identifier ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.Identifier identifier)
        {      
            return new Identifier
            {
                FirstTokenIndex = identifier.FirstTokenIndex,
                LastTokenIndex = identifier.LastTokenIndex,
                Value = identifier.Value,
                QuoteType = ConvertFromScriptDom(identifier.QuoteType)
            };
        }

        private IList<Identifier> ConvertFromScriptDom(IList<Microsoft.SqlServer.TransactSql.ScriptDom.Identifier> identifiers)
        {
            IList<Identifier> res = new List<Identifier>();
            foreach (var identifier in identifiers)
            {
                res.Add(ConvertFromScriptDom(identifier));
            }
            return res;
        }

        private QualifiedJoinType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.QualifiedJoinType qualifiedJoinType)
        {
            switch (qualifiedJoinType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.QualifiedJoinType.FullOuter:
                    return QualifiedJoinType.FullOuter;
                case Microsoft.SqlServer.TransactSql.ScriptDom.QualifiedJoinType.Inner:
                    return QualifiedJoinType.Inner;
                case Microsoft.SqlServer.TransactSql.ScriptDom.QualifiedJoinType.LeftOuter:
                    return QualifiedJoinType.LeftOuter;
                case Microsoft.SqlServer.TransactSql.ScriptDom.QualifiedJoinType.RightOuter:
                    return QualifiedJoinType.RightOuter;
                default:
                    throw new NotImplementedException();
            }
        }

        private JoinHint ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.JoinHint joinHint)
        {
            switch (joinHint)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.JoinHint.Hash:
                    return JoinHint.Hash;
                case Microsoft.SqlServer.TransactSql.ScriptDom.JoinHint.Loop:
                    return JoinHint.Loop;
                case Microsoft.SqlServer.TransactSql.ScriptDom.JoinHint.Merge:
                    return JoinHint.Merge;
                case Microsoft.SqlServer.TransactSql.ScriptDom.JoinHint.None:
                    return JoinHint.None;
                case Microsoft.SqlServer.TransactSql.ScriptDom.JoinHint.Remote:
                    return JoinHint.Remote;
                default:
                    throw new NotImplementedException();
            }
        }
        
        private UnqualifiedJoinType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.UnqualifiedJoinType unqualifiedJoinType)
        {
            switch (unqualifiedJoinType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.UnqualifiedJoinType.CrossApply:
                    return UnqualifiedJoinType.CrossApply;
                case Microsoft.SqlServer.TransactSql.ScriptDom.UnqualifiedJoinType.CrossJoin:
                    return UnqualifiedJoinType.CrossJoin;
                case Microsoft.SqlServer.TransactSql.ScriptDom.UnqualifiedJoinType.OuterApply:
                    return UnqualifiedJoinType.OuterApply;
                default:
                    throw new NotImplementedException();
            }
        }

        private SortOrder ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.SortOrder sortOrder)
        {
            switch (sortOrder)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.SortOrder.NotSpecified:
                    return SortOrder.NotSpecified;
                case Microsoft.SqlServer.TransactSql.ScriptDom.SortOrder.Ascending:
                    return SortOrder.Ascending;
                case Microsoft.SqlServer.TransactSql.ScriptDom.SortOrder.Descending:
                    return SortOrder.Descending;
                default:
                    throw new NotImplementedException();
            }
        }
        
        private BooleanBinaryExpressionType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.BooleanBinaryExpressionType booleanBinaryExpressionType)
        {
            switch (booleanBinaryExpressionType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanBinaryExpressionType.And:
                    return BooleanBinaryExpressionType.And;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanBinaryExpressionType.Or:
                    return BooleanBinaryExpressionType.Or;
                default:
                    throw new NotImplementedException();
            }
        }
        
        private BooleanComparisonType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType booleanComparisonType)
        {
            switch (booleanComparisonType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.Equals:
                    return BooleanComparisonType.Equals;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.GreaterThan:
                    return BooleanComparisonType.GreaterThan;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.GreaterThanOrEqualTo:
                    return BooleanComparisonType.GreaterThanOrEqualTo;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.LeftOuterJoin:
                    return BooleanComparisonType.LeftOuterJoin;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.LessThan:
                    return BooleanComparisonType.LessThan;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.LessThanOrEqualTo:
                    return BooleanComparisonType.LessThanOrEqualTo;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.NotEqualToBrackets:
                    return BooleanComparisonType.NotEqualToBrackets;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.NotEqualToExclamation:
                    return BooleanComparisonType.NotEqualToExclamation;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.NotGreaterThan:
                    return BooleanComparisonType.NotGreaterThan;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.NotLessThan:
                    return BooleanComparisonType.NotLessThan;
                case Microsoft.SqlServer.TransactSql.ScriptDom.BooleanComparisonType.RightOuterJoin:
                    return BooleanComparisonType.RightOuterJoin;
                default:
                    throw new NotImplementedException();
            }
        }

        private SubqueryComparisonPredicateType ConvertFromScriptDom(Microsoft.SqlServer.TransactSql.ScriptDom.SubqueryComparisonPredicateType subqueryComparisonPredicateType)
        {
            switch (subqueryComparisonPredicateType)
            {
                case Microsoft.SqlServer.TransactSql.ScriptDom.SubqueryComparisonPredicateType.All:
                    return SubqueryComparisonPredicateType.All;
                case Microsoft.SqlServer.TransactSql.ScriptDom.SubqueryComparisonPredicateType.Any:
                    return SubqueryComparisonPredicateType.Any;
                case Microsoft.SqlServer.TransactSql.ScriptDom.SubqueryComparisonPredicateType.None:
                    return SubqueryComparisonPredicateType.None;
                default:
                    throw new NotImplementedException();
            }
        }

    }

    internal class SelectVisitor : TSqlConcreteFragmentVisitor
    {
        public readonly List<SelectStatement> SelectStatements = new List<SelectStatement>();

        public override void Visit(SelectStatement node)
        {
            SelectStatements.Add(node);
        }
    }

}