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
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class WSqlParser
    {
        internal TSql110Parser tsqlParser;
        private IList<TSqlParserToken> _tokens;

        public WSqlParser()
        {
            tsqlParser = new TSql110Parser(true);
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
            WSqlStatement wstat;

            switch (tsqlStat.GetType().Name)
            {
                case "SelectStatement":
                    {
                        var sel = tsqlStat as SelectStatement;
                        wstat = ParseSelectQueryStatement(sel.QueryExpression);

                        break;
                    }
                case "CreateFunctionStatement":
                    {
                        var creat = tsqlStat as CreateFunctionStatement;
                        var wcreat = new WCreateFunctionStatement
                        {
                            Parameters = creat.Parameters,
                            ReturnType = creat.ReturnType,
                            StatementList = new List<WSqlStatement>(creat.StatementList.Statements.Count),
                            FirstTokenIndex = creat.FirstTokenIndex,
                            LastTokenIndex = creat.LastTokenIndex,
                            Name = ParseSchemaObjectName(creat.Name)
                        };

                        foreach (var stat in creat.StatementList.Statements)
                        {
                            wcreat.StatementList.Add(ParseStatement(stat));
                        }

                        wstat = wcreat;
                        break;
                    }
                case "BeginEndBlockStatement":
                    {
                        var bestat = tsqlStat as BeginEndBlockStatement;
                        var wbestat = new WBeginEndBlockStatement
                        {
                            StatementList = new List<WSqlStatement>(bestat.StatementList.Statements.Count),
                            FirstTokenIndex = bestat.FirstTokenIndex,
                            LastTokenIndex = bestat.LastTokenIndex
                        };

                        foreach (var pstat in bestat.StatementList.Statements.Select(ParseStatement))
                        {
                            wbestat.StatementList.Add(pstat);
                        }

                        wstat = wbestat;
                        break;
                    }
                case "UpdateStatement":
                    {
                        var upd = tsqlStat as UpdateStatement;
                        wstat = ParseUpdateStatement(upd.UpdateSpecification);
                        break;
                    }
                case "DeleteStatement":
                    {
                        var del = tsqlStat as DeleteStatement;
                        wstat = ParseDeleteStatement(del.DeleteSpecification);
                        break;
                    }
                case "InsertStatement":
                    {
                        var ins = tsqlStat as InsertStatement;
                        wstat = ParseInsertStatement(ins.InsertSpecification);
                        break;
                    }
                case "CreateTableStatement":
                    {
                        var cts = tsqlStat as CreateTableStatement;
                        var wcstat = new WCreateTableStatement
                        {
                            FirstTokenIndex = cts.FirstTokenIndex,
                            LastTokenIndex = cts.LastTokenIndex,
                            Definition = ParseTableDefinition(cts.Definition),
                            SchemaObjectName = ParseSchemaObjectName(cts.SchemaObjectName),
                        };
                        wstat = wcstat;
                        break;
                    }
                case "DropTableStatement":
                    {
                        var dts = tsqlStat as DropTableStatement;
                        var wdstat = new WDropTableStatement
                        {
                            FirstTokenIndex = dts.FirstTokenIndex,
                            LastTokenIndex = dts.LastTokenIndex,
                        };
                        if (dts.Objects != null)
                        {
                            wdstat.Objects = new List<WSchemaObjectName>();
                            foreach (var obj in dts.Objects)
                            {
                                wdstat.Objects.Add(ParseSchemaObjectName(obj));
                            }
                        }
                        wstat = wdstat;
                        break;
                    }
                case "CreateViewStatement":
                {
                    var cvs = tsqlStat as CreateViewStatement;
                    var wcvs = new WCreateViewStatement
                    {
                        Columns = cvs.Columns,
                        FirstTokenIndex = cvs.FirstTokenIndex,
                        LastTokenIndex = cvs.LastTokenIndex,
                        SchemaObjectName = ParseSchemaObjectName(cvs.SchemaObjectName),
                        SelectStatement = ParseSelectQueryStatement(cvs.SelectStatement.QueryExpression),
                        ViewOptions = cvs.ViewOptions,
                        WithCheckOption = cvs.WithCheckOption
                    };
                    wstat = wcvs;
                    break;
                }
                case "BeginTransactionStatement":
                    {
                        var beginTranStat = tsqlStat as BeginTransactionStatement;
                        wstat = new WBeginTransactionStatement
                        {
                            Name = ParseIdentifierOrValueExpression(beginTranStat.Name),
                            Distributed = beginTranStat.Distributed,
                            FirstTokenIndex = beginTranStat.FirstTokenIndex,
                            LastTokenIndex = beginTranStat.LastTokenIndex
                        };
                        break;
                    }
                case "CommitTransactionStatement":
                    {

                        var commitTranStat = tsqlStat as CommitTransactionStatement;
                        wstat = new WCommitTransactionStatement
                        {
                            Name = ParseIdentifierOrValueExpression(commitTranStat.Name),
                            FirstTokenIndex = commitTranStat.FirstTokenIndex,
                            LastTokenIndex = commitTranStat.LastTokenIndex
                        };
                        break;
                    }
                case "RollbackTransactionStatement":
                    {

                        var rollbackTranStat = tsqlStat as RollbackTransactionStatement;
                        wstat = new WRollbackTransactionStatement
                        {
                            Name = ParseIdentifierOrValueExpression(rollbackTranStat.Name),
                            FirstTokenIndex = rollbackTranStat.FirstTokenIndex,
                            LastTokenIndex = rollbackTranStat.LastTokenIndex
                        };
                        break;
                    }
                case "SaveTransactionStatement":
                    {

                        var saveTranStat = tsqlStat as SaveTransactionStatement;
                        wstat = new WSaveTransactionStatement
                        {
                            Name = ParseIdentifierOrValueExpression(saveTranStat.Name),
                            FirstTokenIndex = saveTranStat.FirstTokenIndex,
                            LastTokenIndex = saveTranStat.LastTokenIndex
                        };
                        break;
                    }
                case "CreateProcedureStatement":
                    {
                        var creat = tsqlStat as CreateProcedureStatement;
                        var wcreat = new WCreateProcedureStatement
                        {
                            IsForReplication = creat.IsForReplication,
                            Parameters = creat.Parameters,
                            StatementList = new List<WSqlStatement>(creat.StatementList.Statements.Count),
                            FirstTokenIndex = creat.FirstTokenIndex,
                            LastTokenIndex = creat.LastTokenIndex,
                            ProcedureReference = new WProcedureReference
                            {
                                Name = ParseSchemaObjectName(creat.ProcedureReference.Name),
                                Number = creat.ProcedureReference.Number
                            }
                        };

                        foreach (var stat in creat.StatementList.Statements)
                        {
                            wcreat.StatementList.Add(ParseStatement(stat));
                        }

                        wstat = wcreat;
                        break;
                    }
                case "DropProcedureStatement":
                    {
                        var dts = tsqlStat as DropProcedureStatement;
                        var wdstat = new WDropProcedureStatement
                        {
                            FirstTokenIndex = dts.FirstTokenIndex,
                            LastTokenIndex = dts.LastTokenIndex,
                        };
                        if (dts.Objects != null)
                        {
                            wdstat.Objects = new List<WSchemaObjectName>();
                            foreach (var obj in dts.Objects)
                            {
                                wdstat.Objects.Add(ParseSchemaObjectName(obj));
                            }
                        }
                        wstat = wdstat;
                        break;
                    }
                case "WhileStatement":
                    {
                        var bestat = tsqlStat as WhileStatement;
                        var wbestat = new WWhileStatement()
                        {
                            Predicate = ParseBooleanExpression(bestat.Predicate),
                            Statement = ParseStatement(bestat.Statement),
                            FirstTokenIndex = bestat.FirstTokenIndex,
                            LastTokenIndex = bestat.LastTokenIndex
                        };


                        wstat = wbestat;
                        break;
                    }
                case "DeclareVariableStatement":
                {
                    var dvstat = tsqlStat as DeclareVariableStatement;
                    wstat = new WDeclareVariableStatement
                    {
                        Statement = dvstat
                    };
                    break;
                }
                default:
                    {
                        wstat = new WSqlUnknownStatement(tsqlStat)
                        {
                            FirstTokenIndex = tsqlStat.FirstTokenIndex,
                            LastTokenIndex = tsqlStat.LastTokenIndex
                        };

                        break;
                    }
            }

            return wstat;
        }

        private WTableDefinition ParseTableDefinition(TableDefinition tableDef)
        {
            if (tableDef == null)
                return null;
            var wTableDef = new WTableDefinition
            {
                FirstTokenIndex = tableDef.FirstTokenIndex,
                LastTokenIndex = tableDef.LastTokenIndex,
            };

            if (tableDef.ColumnDefinitions != null)
            {
                wTableDef.ColumnDefinitions = new List<WColumnDefinition>(tableDef.ColumnDefinitions.Count);
                foreach (var colDef in tableDef.ColumnDefinitions)
                    wTableDef.ColumnDefinitions.Add(ParseColumnDefinition(colDef));
            }

            if (tableDef.TableConstraints != null)
            {
                wTableDef.TableConstraints = new List<WConstraintDefinition>(tableDef.TableConstraints.Count);
                foreach (var tableCon in tableDef.TableConstraints)
                    wTableDef.TableConstraints.Add(ParseConstraintDefinition(tableCon));
            }

            if (tableDef.Indexes != null)
            {
                wTableDef.Indexes = new List<WIndexDefinition>(tableDef.Indexes.Count);
                foreach (var idx in tableDef.Indexes)
                    wTableDef.Indexes.Add(ParseIndexDefinition(idx));
            }
            return wTableDef;
        }

        private WColumnDefinition ParseColumnDefinition(ColumnDefinition columnDef)
        {
            if (columnDef == null)
                return null;
            var wColumnDef = new WColumnDefinition
            {
                FirstTokenIndex = columnDef.FirstTokenIndex,
                LastTokenIndex = columnDef.LastTokenIndex,
                ColumnIdentifier = columnDef.ColumnIdentifier,
                DataType = ParseDataType(columnDef.DataType),
                Collation = columnDef.Collation,
                ComputedColumnExpression = ParseScalarExpression(columnDef.ComputedColumnExpression),
                StorageOptions = columnDef.StorageOptions,
                Index = ParseIndexDefinition(columnDef.Index),
            };
            if (columnDef.Constraints != null)
            {
                wColumnDef.Constraints = new List<WConstraintDefinition>(columnDef.Constraints.Count);
                foreach (var con in columnDef.Constraints)
                    wColumnDef.Constraints.Add(ParseConstraintDefinition(con));
            }
            if (columnDef.IdentityOptions != null)
                wColumnDef.IdentityOptions = new WIdentityOptions
                {
                    FirstTokenIndex = columnDef.IdentityOptions.FirstTokenIndex,
                    LastTokenIndex = columnDef.IdentityOptions.LastTokenIndex,
                    IdentitySeed = ParseScalarExpression(columnDef.IdentityOptions.IdentitySeed),
                    IdentityIncrement = ParseScalarExpression(columnDef.IdentityOptions.IdentityIncrement),
                    IsIdentityNotForReplication = columnDef.IdentityOptions.IsIdentityNotForReplication,
                };
            return wColumnDef;
        }

        private WConstraintDefinition ParseConstraintDefinition(ConstraintDefinition consDef)
        {
            if (consDef == null)
                return null;
            WConstraintDefinition wConsDef = null;
            switch (consDef.GetType().Name)
            {
                case "CheckConstraintDefinition":
                    {
                        var checkConsDef = consDef as CheckConstraintDefinition;
                        wConsDef = new WCheckConstraintDefinition
                        {
                            ConstraintIdentifier = checkConsDef.ConstraintIdentifier,
                            CheckCondition = ParseBooleanExpression(checkConsDef.CheckCondition),
                            NotForReplication = checkConsDef.NotForReplication
                        };
                        break;
                    }
                case "DefaultConstraintDefinition":
                    {
                        var defaultConsDef = consDef as DefaultConstraintDefinition;
                        wConsDef = new WDefaultConstraintDefinition
                        {
                            ConstraintIdentifier = defaultConsDef.ConstraintIdentifier,
                            Expression = ParseScalarExpression(defaultConsDef.Expression),
                            Column = defaultConsDef.Column,
                            WithValues = defaultConsDef.WithValues,
                        };
                        break;
                    }
                case "ForeignKeyConstraintDefinition":
                    {
                        var foreignConsDef = consDef as ForeignKeyConstraintDefinition;
                        var wForeignConsDef = new WForeignKeyConstraintDefinition
                        {
                            ConstraintIdentifier = foreignConsDef.ConstraintIdentifier,
                            ReferenceTableName = ParseSchemaObjectName(foreignConsDef.ReferenceTableName),
                            DeleteAction = foreignConsDef.DeleteAction,
                            UpdateAction = foreignConsDef.UpdateAction,
                            NotForReplication = foreignConsDef.NotForReplication,
                        };
                        if (foreignConsDef.Columns != null)
                        {
                            wForeignConsDef.Columns = new List<Identifier>(foreignConsDef.Columns.Count);
                            foreach (var col in foreignConsDef.Columns)
                                wForeignConsDef.Columns.Add(col);
                        }

                        if (foreignConsDef.ReferencedTableColumns != null)
                        {
                            wForeignConsDef.ReferencedTableColumns = new List<Identifier>(foreignConsDef.ReferencedTableColumns.Count);
                            foreach (var col in foreignConsDef.ReferencedTableColumns)
                                wForeignConsDef.ReferencedTableColumns.Add(col);
                        }
                        wConsDef = wForeignConsDef;

                        break;
                    }
                case "NullableConstraintDefinition":
                    {
                        var nullConsDef = consDef as NullableConstraintDefinition;
                        wConsDef = new WNullableConstraintDefinition
                        {
                            ConstraintIdentifier = nullConsDef.ConstraintIdentifier,
                            Nullable = nullConsDef.Nullable,
                        };
                        break;
                    }
                case "UniqueConstraintDefinition":
                    {
                        var uniqConsDef = consDef as UniqueConstraintDefinition;
                        var wUniqConsDef = new WUniqueConstraintDefinition
                        {
                            ConstraintIdentifier = uniqConsDef.ConstraintIdentifier,
                            Clustered = uniqConsDef.Clustered,
                            IsPrimaryKey = uniqConsDef.IsPrimaryKey,
                        };
                        if (uniqConsDef.Columns != null)
                        {
                            wUniqConsDef.Columns = new List<Tuple<WColumnReferenceExpression, SortOrder>>();
                            foreach (var col in uniqConsDef.Columns)
                            {
                                wUniqConsDef.Columns.Add(new Tuple<WColumnReferenceExpression, SortOrder>(
                                    new WColumnReferenceExpression
                                    {
                                        ColumnType = col.Column.ColumnType,
                                        MultiPartIdentifier = ParseMultiPartIdentifier(col.Column.MultiPartIdentifier),
                                        FirstTokenIndex = col.Column.FirstTokenIndex,
                                        LastTokenIndex = col.Column.LastTokenIndex,
                                    },
                                    col.SortOrder));
                            }
                        }
                        wConsDef = wUniqConsDef;
                        break;
                    }
            }
            return wConsDef;
        }

        private WIndexDefinition ParseIndexDefinition(IndexDefinition idxDef)
        {
            if (idxDef == null)
                return null;
            var wIdxDef = new WIndexDefinition
            {
                FirstTokenIndex = idxDef.FirstTokenIndex,
                LastTokenIndex = idxDef.LastTokenIndex,
                IndexType = idxDef.IndexType,
                Name = idxDef.Name,
            };
            if (idxDef.Columns == null)
                return wIdxDef;

            wIdxDef.Columns = new List<Tuple<WColumnReferenceExpression, SortOrder>>();
            foreach (var col in idxDef.Columns)
            {
                wIdxDef.Columns.Add(new Tuple<WColumnReferenceExpression, SortOrder>(
                    new WColumnReferenceExpression
                    {
                        ColumnType = col.Column.ColumnType,
                        MultiPartIdentifier = ParseMultiPartIdentifier(col.Column.MultiPartIdentifier),
                        FirstTokenIndex = col.Column.FirstTokenIndex,
                        LastTokenIndex = col.Column.LastTokenIndex,
                    },
                    col.SortOrder));
            }
            return wIdxDef;
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

        private WSqlStatement ParseInsertStatement(InsertSpecification insSpec)
        {
            var winsSpec = new WInsertSpecification
            {
                Target = ParseTableReference(insSpec.Target),
                InsertOption = insSpec.InsertOption,
                InsertSource = ParseInsertSource(insSpec.InsertSource),
                FirstTokenIndex = insSpec.FirstTokenIndex,
                LastTokenIndex = insSpec.LastTokenIndex
            };

            if (insSpec.TopRowFilter != null)
            {
                winsSpec.TopRowFilter = new WTopRowFilter
                {
                    Expression = ParseScalarExpression(insSpec.TopRowFilter.Expression),
                    WithTies = insSpec.TopRowFilter.WithTies,
                    Percent = insSpec.TopRowFilter.Percent,
                    FirstTokenIndex = insSpec.TopRowFilter.FirstTokenIndex,
                    LastTokenIndex = insSpec.TopRowFilter.LastTokenIndex
                };
            }

            //Columns
            winsSpec.Columns = new List<WColumnReferenceExpression>(insSpec.Columns.Count);
            foreach (var wexpr in insSpec.Columns.Select(column => new WColumnReferenceExpression
            {
                MultiPartIdentifier = ParseMultiPartIdentifier(column.MultiPartIdentifier),
                ColumnType = column.ColumnType,
                FirstTokenIndex = column.FirstTokenIndex,
                LastTokenIndex = column.LastTokenIndex
            }))
            {
                winsSpec.Columns.Add(wexpr);
            }

            return winsSpec;
        }

        private WInsertSource ParseInsertSource(InsertSource insSource)
        {
            if (insSource == null)
                return null;

            WInsertSource winsSouce = null;
            switch (insSource.GetType().Name)
            {
                case "SelectInsertSource":
                    {
                        var selInsSource = insSource as SelectInsertSource;
                        var wselInsSource = new WSelectInsertSource
                        {
                            Select = ParseSelectQueryStatement(selInsSource.Select),
                            FirstTokenIndex = selInsSource.FirstTokenIndex,
                            LastTokenIndex = selInsSource.LastTokenIndex

                        };
                        winsSouce = wselInsSource;
                        break;
                    }
                case "ValuesInsertSource":
                    {
                        var valInsSource = insSource as ValuesInsertSource;
                        var wvalInsSource = new WValuesInsertSource
                        {
                            IsDefaultValues = valInsSource.IsDefaultValues,
                            RowValues = new List<WRowValue>(valInsSource.RowValues.Count),
                            FirstTokenIndex = valInsSource.FirstTokenIndex,
                            LastTokenIndex = valInsSource.LastTokenIndex
                        };

                        foreach (var rowValue in valInsSource.RowValues)
                        {
                            wvalInsSource.RowValues.Add(ParseRowValue(rowValue));
                        }

                        winsSouce = wvalInsSource;
                        break;
                    }
            }

            return winsSouce;
        }

        private WRowValue ParseRowValue(RowValue rowValue)
        {
            var wrowValue = new WRowValue
            {
                ColumnValues = new List<WScalarExpression>(rowValue.ColumnValues.Count),
                FirstTokenIndex = rowValue.FirstTokenIndex,
                LastTokenIndex = rowValue.LastTokenIndex
            };

            foreach (var expr in rowValue.ColumnValues)
            {
                wrowValue.ColumnValues.Add(ParseScalarExpression(expr));
            }

            return wrowValue;
        }

        private WSqlStatement ParseDeleteStatement(DeleteSpecification delSpec)
        {
            if (delSpec == null)
                return null;

            var wdelSpec = new WDeleteSpecification
            {
                Target = ParseTableReference(delSpec.Target),
                FirstTokenIndex = delSpec.FirstTokenIndex,
                LastTokenIndex = delSpec.LastTokenIndex
            };

            //From Clause
            if (delSpec.FromClause != null && delSpec.FromClause.TableReferences != null)
            {
                wdelSpec.FromClause = new WFromClause
                {
                    FirstTokenIndex = delSpec.FromClause.FirstTokenIndex,
                    LastTokenIndex = delSpec.FromClause.LastTokenIndex,
                    TableReferences = new List<WTableReference>(delSpec.FromClause.TableReferences.Count)
                };
                foreach (var pref in delSpec.FromClause.TableReferences.Select(ParseTableReference).Where(pref => pref != null))
                {
                    wdelSpec.FromClause.TableReferences.Add(pref);
                }
            }

            //where clause
            if (delSpec.WhereClause != null && delSpec.WhereClause.SearchCondition != null)
            {
                wdelSpec.WhereClause = new WWhereClause
                {
                    FirstTokenIndex = delSpec.WhereClause.FirstTokenIndex,
                    LastTokenIndex = delSpec.WhereClause.LastTokenIndex,
                    SearchCondition = ParseBooleanExpression(delSpec.WhereClause.SearchCondition)
                };
            }

            //top row filter
            if (delSpec.TopRowFilter != null)
            {
                wdelSpec.TopRowFilter = new WTopRowFilter
                {
                    Expression = ParseScalarExpression(delSpec.TopRowFilter.Expression),
                    Percent = delSpec.TopRowFilter.Percent,
                    WithTies = delSpec.TopRowFilter.WithTies,
                    FirstTokenIndex = delSpec.TopRowFilter.FirstTokenIndex,
                    LastTokenIndex = delSpec.TopRowFilter.LastTokenIndex
                };
            }

            return wdelSpec;
        }

        private WSqlStatement ParseUpdateStatement(UpdateSpecification upSpec)
        {
            if (upSpec == null)
                return null;
            var wupSpec = new WUpdateSpecification
            {
                Target = ParseTableReference(upSpec.Target),
                FirstTokenIndex = upSpec.FirstTokenIndex,
                LastTokenIndex = upSpec.LastTokenIndex
            };

            //TopRowFilter
            if (upSpec.TopRowFilter != null)
            {
                wupSpec.TopRowFilter = new WTopRowFilter
                {
                    Percent = upSpec.TopRowFilter.Percent,
                    WithTies = upSpec.TopRowFilter.WithTies,
                    Expression = ParseScalarExpression(upSpec.TopRowFilter.Expression),
                    FirstTokenIndex = upSpec.TopRowFilter.FirstTokenIndex,
                    LastTokenIndex = upSpec.TopRowFilter.LastTokenIndex
                };
            }

            //From Clause
            if (upSpec.FromClause != null && upSpec.FromClause.TableReferences != null)
            {
                wupSpec.FromClause = new WFromClause
                {
                    FirstTokenIndex = upSpec.FromClause.FirstTokenIndex,
                    LastTokenIndex = upSpec.FromClause.LastTokenIndex,
                    TableReferences = new List<WTableReference>(upSpec.FromClause.TableReferences.Count)
                };
                foreach (var pref in upSpec.FromClause.TableReferences.Select(ParseTableReference).Where(pref => pref != null))
                {
                    wupSpec.FromClause.TableReferences.Add(pref);
                }
            }

            //Where Clause
            if (upSpec.WhereClause != null && upSpec.WhereClause.SearchCondition != null)
            {
                wupSpec.WhereClause = new WWhereClause
                {
                    FirstTokenIndex = upSpec.WhereClause.FirstTokenIndex,
                    LastTokenIndex = upSpec.WhereClause.LastTokenIndex,
                    SearchCondition = ParseBooleanExpression(upSpec.WhereClause.SearchCondition)
                };
            }

            //Set Clauses
            IList<WSetClause> wsetClauses = new List<WSetClause>(upSpec.SetClauses.Count);
            foreach (var setClause in upSpec.SetClauses)
            {
                WSetClause wsetClause;
                switch (setClause.GetType().Name)
                {
                    case "AssignmentSetClause":
                        {
                            var asSetClause = setClause as AssignmentSetClause;
                            wsetClause = ParseAssignmentSetClause(asSetClause);
                            break;
                        }
                    case "FunctionCallSetClause":
                    {
                        var fcSetClause = setClause as FunctionCallSetClause;
                        var mtFunction = fcSetClause.MutatorFunction;
                        wsetClause = new WFunctionCallSetClause
                        {
                            MutatorFuction = ParseScalarExpression(mtFunction) as WFunctionCall
                        };
                        break;
                    }
                    default:
                        continue;
                }
                wsetClauses.Add(wsetClause);
            }
            wupSpec.SetClauses = wsetClauses;

            return wupSpec;
        }

        private WSetClause ParseAssignmentSetClause(AssignmentSetClause asSetClause)
        {
            var wasSetClause = new WAssignmentSetClause
            {
                AssignmentKind = asSetClause.AssignmentKind,
                FirstTokenIndex = asSetClause.FirstTokenIndex,
                LastTokenIndex = asSetClause.LastTokenIndex
            };

            if (asSetClause.Column != null)
            {
                var wexpr = new WColumnReferenceExpression
                {
                    MultiPartIdentifier = ParseMultiPartIdentifier(asSetClause.Column.MultiPartIdentifier),
                    ColumnType = asSetClause.Column.ColumnType,
                    FirstTokenIndex = asSetClause.Column.FirstTokenIndex,
                    LastTokenIndex = asSetClause.Column.LastTokenIndex
                };
                wasSetClause.Column = wexpr;
            }

            if (asSetClause.NewValue != null)
                wasSetClause.NewValue = ParseScalarExpression(asSetClause.NewValue);
            if (asSetClause.Variable != null)
                wasSetClause.Variable = asSetClause.Variable.Name;

            return wasSetClause;
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

                        pQueryExpr.UniqueRowFilter = qs.UniqueRowFilter;

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
                            AssignmentType = ssv.AssignmentKind,
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
                            ExpressionType = bexpr.BinaryExpressionType,
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
                            ExpressionType = uexpr.UnaryExpressionType,
                            FirstTokenIndex = uexpr.FirstTokenIndex,
                            LastTokenIndex = uexpr.LastTokenIndex
                        };

                        return wuexpr;
                    }
                case "ColumnReferenceExpression":
                    {
                        var cre = scalarExpr as ColumnReferenceExpression;
                        var wexpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = ParseMultiPartIdentifier(cre.MultiPartIdentifier),
                            ColumnType = cre.ColumnType,
                            FirstTokenIndex = cre.FirstTokenIndex,
                            LastTokenIndex = cre.LastTokenIndex
                        };

                        return wexpr;
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
                            FunctionName = fc.FunctionName,
                            UniqueRowFilter = fc.UniqueRowFilter,
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
            if (tabRefWithAlias!=null && tabRefWithAlias.Alias!=null &&
                 GraphViewKeywords._keywords.Contains(tabRefWithAlias.Alias.Value))
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
                        if (oref.SchemaObject.BaseIdentifier.QuoteType == QuoteType.NotQuoted &&
                            (oref.SchemaObject.BaseIdentifier.Value[0] == '@' ||
                             oref.SchemaObject.BaseIdentifier.Value[0] == '#'))
                        {
                            var pref = new WSpecialNamedTableReference
                            {
                                Alias = oref.Alias,
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
                                Alias = oref.Alias,
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
                            Alias = oref.Alias,
                            Columns = oref.Columns,
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
                            Alias = oref.Alias,
                            Columns = oref.Columns,
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
                            QualifiedJoinType = oref.QualifiedJoinType,
                            JoinHint = oref.JoinHint,
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
                            UnqualifiedJoinType = oref.UnqualifiedJoinType,
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
                                     SortOrder = e.SortOrder,
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
                            BooleanExpressionType = oexpr.BinaryExpressionType,
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
                            ComparisonType = oexpr.ComparisonType,
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
                            ComparisonType = oexpr.ComparisonType,
                            Expression = ParseScalarExpression(oexpr.Expression),
                            SubqueryComparisonType = oexpr.SubqueryComparisonPredicateType,
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
                LastTokenIndex = name.LastTokenIndex,
                Identifiers = name.Identifiers,
            };
            if (name.Identifiers != null)
            {
                wMultiPartIdentifier.Identifiers = new List<Identifier>();
                foreach (var identifier in name.Identifiers)
                {
                    if (GraphViewKeywords._keywords.Contains(identifier.Value))
                    {
                        var token = _tokens[identifier.FirstTokenIndex];
                        throw new SyntaxErrorException(token.Line, identifier.Value,
                            "System restricted Name cannot be used");
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
                    if (GraphViewKeywords._keywords.Contains(identifier.Value))
                    {
                        var token = _tokens[identifier.FirstTokenIndex];
                        throw new SyntaxErrorException(token.Line, identifier.Value,
                            "System restricted Name cannot be used");
                    }
                    wSchemaObjectName.Identifiers.Add(identifier);
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

        private WIdentifierOrValueExpression ParseIdentifierOrValueExpression(IdentifierOrValueExpression value)
        {
            if (value == null)
                return null;
            if (GraphViewKeywords._keywords.Contains(value.Identifier.Value))
            {
                var token = _tokens[value.FirstTokenIndex];
                throw new SyntaxErrorException(token.Line, value.Identifier.Value,
                    "System restricted Name cannot be used");
            }
            return new WIdentifierOrValueExpression
            {
                FirstTokenIndex = value.FirstTokenIndex,
                LastTokenIndex = value.LastTokenIndex,
                Identifier = value.Identifier,
                ValueExpression = ParseScalarExpression(value.ValueExpression) as WValueExpression
            };
        }
    }

}
