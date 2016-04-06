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
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Remoting.Contexts;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class TranslateDataModificationVisitor : WSqlFragmentVisitor
    {
        public SqlTransaction Tx { get; private set; }

        public TranslateDataModificationVisitor(SqlTransaction tx)
        {
            this.Tx = tx;
        }
        public void Invoke(WSqlFragment fragment)
        {
            fragment.Accept(this);
        }

        public override void Visit(WSqlBatch node)
        {
            base.Visit(node);
            node.Statements = Translate(node.Statements);
        }

        public override void Visit(WProcedureStatement node)
        {
            base.Visit(node);
            node.StatementList = Translate(node.StatementList);
        }

        public override void Visit(WBeginEndBlockStatement node)
        {
            base.Visit(node);
            node.StatementList = Translate(node.StatementList);
        }

        private List<WSqlStatement> Translate(IList<WSqlStatement> statements)
        {
            var result = new List<WSqlStatement>();
            var index = 0;
            for (var count = statements.Count; index < count; ++index)
            {
                var insertEdgeStatement = statements[index] as WInsertEdgeSpecification;
                if (insertEdgeStatement != null)
                {
                    //result.Add(new WBeginTransactionStatement
                    //{
                    //    Name = new WIdentifierOrValueExpression
                    //    {
                    //        Identifier = new Identifier { Value = "InsertEdgeTran" }
                    //    }
                    //});
                    
                    //result.Add(TranslateInsertEdge(insertEdgeStatement, false));
                    //result.Add(TranslateInsertEdge(insertEdgeStatement, true));

                    var stmts = new List<WSqlStatement>();
                    TranslateEdgeInsert(insertEdgeStatement, stmts);
                    result.Add(new WBeginEndBlockStatement() { StatementList = stmts, });

                    //result.Add(new WCommitTransactionStatement
                    //{
                    //    Name = new WIdentifierOrValueExpression
                    //    {
                    //        Identifier = new Identifier { Value = "InsertEdgeTran" }
                    //    }
                    //});
                    continue;
                }
                var deleteEdgeStatement = statements[index] as WDeleteEdgeSpecification;
                if (deleteEdgeStatement != null)
                {
                    //result.Add(new WBeginTransactionStatement
                    //{
                    //    Name = new WIdentifierOrValueExpression
                    //    {
                    //        Identifier = new Identifier { Value = "DeleteEdgeTran" }
                    //    }
                    //});
                    //result.Add(TranslateDeleteEdge(deleteEdgeStatement, true));
                    //result.Add(TranslateDeleteEdge(deleteEdgeStatement, false));

                    var stmts = new List<WSqlStatement>();
                    TranslateEdgeDelete(deleteEdgeStatement, stmts);
                    result.Add(new WBeginEndBlockStatement() { StatementList = stmts, });

                    //result.Add(new WCommitTransactionStatement
                    //{
                    //    Name = new WIdentifierOrValueExpression
                    //    {
                    //        Identifier = new Identifier { Value = "DeleteEdgeTran" }
                    //    }
                    //});
                    continue;
                }
                var deleteNodeStatement = statements[index] as WDeleteNodeSpecification;
                if (deleteNodeStatement != null)
                {
                    TranslateNodeDelete(deleteNodeStatement, result);
                    continue;
                }

                result.Add(statements[index]);
            }
            return result;
        }

        private void CheckInsertEdgeValidity(string tableSchema, string sourceTableName, string columnName, string sinkTableName)
        {
            using (var command = Tx.Connection.CreateCommand())
            {
                command.Transaction = Tx;
                command.Parameters.AddWithValue("@schema", tableSchema);
                command.Parameters.AddWithValue("@tableName", sourceTableName);
                command.Parameters.AddWithValue("@colName", columnName);
                command.Parameters.AddWithValue("@role", WNodeTableColumnRole.Edge);
                command.CommandText = string.Format(@"
                    SELECT NodeColumn.Reference 
                    FROM [{0}] as NodeTable JOIN [{1}] AS NodeColumn 
                    ON NodeTable.TableId = NodeColumn.TableId
                    WHERE NodeTable.TableSchema = @schema and 
                          NodeTable.TableName = @tableName and 
                          NodeColumn.ColumnName = @colName and
                          NodeColumn.ColumnRole = @role", GraphViewConnection.MetadataTables[0],
                    GraphViewConnection.MetadataTables[1]);
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                        throw new GraphViewException(
                            string.Format("Node table {0} not exists or edge {1} not exists in node table {0}",
                                sourceTableName, columnName));
                    string sinkReference = reader[0].ToString();
                    if (string.Compare(sinkTableName, sinkReference, StringComparison.OrdinalIgnoreCase) != 0)
                        throw new GraphViewException(string.Format("Mismatch sink node table reference {0}",
                            sinkTableName));
                }
            }
        }

        /// <summary>
        /// Translates insert edge statement into update statement.
        /// Uses common table expression to record the inserting adjacency-list, 
        /// then inserts result into a table variable with soucrc id as its primary to ensure the order,
        /// finally update node tables.
        /// </summary>
        /// <param name="node">Insert edge specification</param>
        /// <param name="res">Resulting statements</param>
        private void TranslateEdgeInsert(WInsertEdgeSpecification node,List<WSqlStatement> res)
        {
            const string insertEdgeTableName = "GraphView_InsertEdgeInternalTable";
            string insertEdgeTempTable = "InsertTmp_"+Path.GetRandomFileName().Replace(".", "").Substring(0, 8);

            var sourceNameTable = node.Target as WNamedTableReference;
            if (sourceNameTable == null)
                throw new GraphViewException("Source table of INSERT EDGE statement should be a named table reference.");
            List<WScalarExpression> encodeParameters;
            WTableReference sinkTable;

            // Wraps the select statement in a common table expression
            var edgeSelectQueryVisitor = new InsertEdgeSelectVisitor();
            var insertCommonTableExpr = edgeSelectQueryVisitor.Invoke(node.SelectInsertSource.Select,
                insertEdgeTempTable, sourceNameTable.TableObjectName.BaseIdentifier.Value, out sinkTable,
                out encodeParameters);
            
            var sinkNameTable = sinkTable as WNamedTableReference;
            if (sinkNameTable == null)
                throw new GraphViewException("Sink table of INSERT EDGE statement should be a named table reference.");

            var edgeName = node.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value;
            var tableSchema =
                    sourceNameTable.TableObjectName.SchemaIdentifier != null
                        ? sourceNameTable.TableObjectName.SchemaIdentifier.Value
                        : "dbo";
            var tableName = sourceNameTable.TableObjectName.BaseIdentifier.Value;

            CheckInsertEdgeValidity(tableSchema, tableName, edgeName, sinkNameTable.TableObjectName.BaseIdentifier.Value);



            // Select Into TempTable
            //res.Add(insertCommonTableExpr);

            // Get edge select query and reversed edge select query
            WSelectQueryBlock edgeSelectQuery = new WSelectQueryBlock
            {
                SelectElements = new List<WSelectElement>
                {
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "src"})
                        },
                        ColumnName = "source"
                    },
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WFunctionCall
                        {
                            CallTarget = new WMultiPartIdentifierCallTarget
                            {
                                Identifiers = new WMultiPartIdentifier(new Identifier {Value = "dbo"})
                            },
                            FunctionName = new Identifier {Value = tableSchema + '_' + tableName +'_' + edgeName + '_' + "Encoder"},
                            Parameters = encodeParameters,
                        },
                        ColumnName = "binary1"
                    },
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WFunctionCall
                        {
                            FunctionName = new Identifier{Value = "COUNT"},
                            Parameters = new List<WScalarExpression>
                            {
                                new WColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "sink"})
                                },
                            },
                        },
                        ColumnName = "sinkCnt"
                    }
                },
                FromClause = new WFromClause
                {
                    TableReferences = new List<WTableReference>
                    {
                        new WSpecialNamedTableReference()
                        {
                            TableObjectName = new WSchemaObjectName(new Identifier{Value = insertEdgeTempTable })
                        }
                    }
                },
                GroupByClause = new WGroupByClause
                {
                    GroupingSpecifications = new List<WGroupingSpecification>
                    {
                        new WExpressionGroupingSpec
                        {
                            Expression = new WValueExpression("src",false)
                        }
                    }
                }
            };
            WSelectQueryBlock reversedEdgeSelectQuery = new WSelectQueryBlock
            {
                SelectElements = new List<WSelectElement>
                {
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "sink"})
                        },
                        ColumnName = "source"
                    },
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WFunctionCall
                        {
                            FunctionName =
                                new Identifier
                                {
                                    //Value = GraphViewConnection.GraphViewUdfAssemblyName + "GlobalNodeIdEncoder"
                                    Value = "COUNT"
                                },
                            Parameters = new List<WScalarExpression>
                            {
                                new WColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "src"})
                                },
                            }
                        },
                        ColumnName = "srcCnt"
                    },

                },
                FromClause = edgeSelectQuery.FromClause,
                GroupByClause = new WGroupByClause
                {
                    GroupingSpecifications = new List<WGroupingSpecification>
                    {
                        new WExpressionGroupingSpec
                        {
                            Expression = new WValueExpression("sink", false)
                        }
                    }
                }
            };

            //Check src & sink
            string sourceTableName = sourceNameTable.TableObjectName.ToString();
            string sinkTableName = sinkNameTable.TableObjectName.ToString();
            int compare = string.CompareOrdinal(sourceTableName, sinkTableName);
            WSetClause[] setClauses = new WSetClause[]
            {
                new WFunctionCallSetClause()
                {
                    MutatorFuction = new WFunctionCall
                    {
                        CallTarget = new WMultiPartIdentifierCallTarget()
                        {
                            Identifiers = node.EdgeColumn.MultiPartIdentifier
                        },
                        FunctionName = new Identifier {Value = "WRITE"},
                        Parameters = new WScalarExpression[]
                        {
                            new WColumnReferenceExpression
                            {
                                MultiPartIdentifier = new WMultiPartIdentifier(
                                    new Identifier {Value = insertEdgeTableName},
                                    new Identifier {Value = "binary1"})
                            },
                            new WValueExpression("null", false),
                            new WValueExpression("null", false)
                        }
                    }
                },
                new WAssignmentSetClause()
                {
                    AssignmentKind = AssignmentKind.Equals,
                    Column = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(new Identifier
                            {
                                Value = "InDegree"
                            })
                    },
                    NewValue = new WBinaryExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(new Identifier
                                {
                                    Value = "InDegree"
                                })
                        },
                        SecondExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "srcCnt"})
                        },
                        ExpressionType = BinaryExpressionType.Add
                    }
                },
                new WAssignmentSetClause()
                {
                    AssignmentKind = AssignmentKind.Equals,
                    Column = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(new Identifier
                            {
                                Value = node.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + "OutDegree"
                            })
                    },
                    NewValue = new WBinaryExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(new Identifier
                                {
                                    Value = node.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + "OutDegree"
                                })
                        },
                        SecondExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "sinkCnt"})
                        },
                        ExpressionType = BinaryExpressionType.Add
                    }
                }
            };
            WWhereClause whereClause = new WWhereClause
            {
                SearchCondition = new WBooleanComparisonExpression
                {
                    FirstExpr = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "GlobalNodeId"})
                    },
                    SecondExpr = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier {Value = insertEdgeTableName},
                            new Identifier {Value = "source"}
                            )
                    },
                    ComparisonType = BooleanComparisonType.Equals
                }
            };
            if (compare == 0)
            {
                res.Add(new WDeclareTableVariable
                {
                    VariableName = new Identifier { Value = "@"+insertEdgeTempTable},
                    Definition = new WTableDefinition
                    {
                        ColumnDefinitions = new WColumnDefinition[]
                        {
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "source"},
                                DataType = new WParameterizedDataTypeReference{Name = new WSchemaObjectName(new Identifier{Value = "bigint"})},
                                Constraints = new List<WConstraintDefinition>
                                {
                                    new WUniqueConstraintDefinition
                                    {
                                        Clustered = true,
                                        IsPrimaryKey = true
                                    }
                                }
                            }, 
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "binary1"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier{Value = "varbinary"}),
                                    Parameters = new List<Literal>{new MaxLiteral{Value = "max"}}
                                },
                            }, 
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "sinkCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier{Value = "int"}),
                                },
                            }, 
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "srcCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier{Value = "int"}),
                                },
                            }, 
                        },
                        Indexes = new List<WIndexDefinition>(),
                        TableConstraints = new List<WConstraintDefinition>()
                    }
                });
                res.Add(insertCommonTableExpr);
                var tableVar = new WSpecialNamedTableReference
                {
                    TableObjectName = new WSchemaObjectName(new Identifier {Value = "@" + insertEdgeTempTable})
                };
                res.Add(new WInsertSpecification
                {
                    Target =tableVar,
                    InsertSource = new WSelectInsertSource
                    {
                        Select = new WSelectQueryBlock
                        {
                            SelectElements = new WSelectElement[]
                            {
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WFunctionCall
                                    {
                                        FunctionName = new Identifier {Value = "ISNULL"},
                                        Parameters = new WScalarExpression[]
                                        {
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = insertEdgeTableName + "_1"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            },
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = insertEdgeTableName + "_2"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            },
                                        }
                                    },
                                    ColumnName = "source"
                                },
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WColumnReferenceExpression
                                    {
                                        MultiPartIdentifier =
                                            new WSchemaObjectName(new Identifier[] {new Identifier {Value = "binary1"}}),
                                    }
                                },
                                //new WSelectScalarExpression
                                //{
                                //    SelectExpr = new WColumnReferenceExpression
                                //    {
                                //                MultiPartIdentifier = new WSchemaObjectName(new Identifier[]{new Identifier{Value = "binary2"}}),
                                //    }
                                //}, 
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WFunctionCall
                                    {
                                        FunctionName = new Identifier {Value = "ISNULL"},
                                        Parameters = new WScalarExpression[]
                                        {
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = insertEdgeTableName + "_1"},
                                                        new Identifier {Value = "sinkCnt"}
                                                    }),
                                            },
                                            new WValueExpression
                                            {
                                                Value = "0"
                                            },
                                        }
                                    },
                                    ColumnName = "sinkCnt"
                                },
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WFunctionCall
                                    {
                                        FunctionName = new Identifier {Value = "ISNULL"},
                                        Parameters = new WScalarExpression[]
                                        {
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = insertEdgeTableName + "_2"},
                                                        new Identifier {Value = "srcCnt"}
                                                    }),
                                            },
                                            new WValueExpression
                                            {
                                                Value = "0"
                                            },
                                        }
                                    },
                                    ColumnName = "srcCnt"
                                },
                            },
                            FromClause = new WFromClause
                            {
                                TableReferences = new WTableReference[]
                                {
                                    new WQualifiedJoin
                                    {
                                        QualifiedJoinType = QualifiedJoinType.FullOuter,
                                        FirstTableRef = new WQueryDerivedTable
                                        {
                                            Alias = new Identifier {Value = insertEdgeTableName + "_1"},
                                            QueryExpr = edgeSelectQuery
                                        },
                                        SecondTableRef = new WQueryDerivedTable
                                        {
                                            Alias = new Identifier {Value = insertEdgeTableName + "_2"},
                                            QueryExpr = reversedEdgeSelectQuery
                                        },
                                        JoinCondition = new WBooleanComparisonExpression()
                                        {
                                            ComparisonType = BooleanComparisonType.Equals,
                                            FirstExpr = new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = insertEdgeTableName + "_1"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            },
                                            SecondExpr = new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = insertEdgeTableName + "_2"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            }


                                        }
                                    },
                                }
                            }
                        }
                    }
                });
                res.Add(new WUpdateSpecification
                {
                    Target = node.Target,
                    SetClauses = setClauses,
                    FromClause = new WFromClause
                    {
                        TableReferences = new WTableReference[]
                        {
                            new WQueryDerivedTable
                            {
                                Alias = new Identifier{Value = insertEdgeTableName},
                                QueryExpr = new WSelectQueryBlock
                                {
                                    SelectElements = new List<WSelectElement>()
                                    {
                                        new WSelectStarExpression()
                                    },
                                    FromClause = new WFromClause
                                    {
                                        TableReferences = new List<WTableReference>{tableVar}
                                    }
                                }
                            }, 
                        }
                    },
                    WhereClause = whereClause,
                    TopRowFilter = node.TopRowFilter,
                });
                res.Add(new WSqlUnknownStatement
                {
                    Statement = String.Format("DELETE FROM @{0}", insertEdgeTempTable)
                });
            }
            else
            {
                // Declare table variables
                res.Add(new WDeclareTableVariable
                {
                    VariableName = new Identifier {Value = "@" + insertEdgeTempTable+"_1"},
                    Definition = new WTableDefinition
                    {
                        ColumnDefinitions = new WColumnDefinition[]
                        {
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "source"},
                                DataType =
                                    new WParameterizedDataTypeReference
                                    {
                                        Name = new WSchemaObjectName(new Identifier {Value = "bigint"})
                                    },
                                Constraints = new List<WConstraintDefinition>
                                {
                                    new WUniqueConstraintDefinition
                                    {
                                        Clustered = true,
                                        IsPrimaryKey = true
                                    }
                                }
                            },
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "binary1"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier {Value = "varbinary"}),
                                    Parameters = new List<Literal> {new MaxLiteral {Value = "max"}}
                                },
                            },
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "sinkCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier {Value = "int"}),
                                },
                            },
                        },
                        Indexes = new List<WIndexDefinition>(),
                        TableConstraints = new List<WConstraintDefinition>()
                    }
                });
                res.Add(new WDeclareTableVariable
                {
                    VariableName = new Identifier {Value = "@" + insertEdgeTempTable+"_2"},
                    Definition = new WTableDefinition
                    {
                        ColumnDefinitions = new WColumnDefinition[]
                        {
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "source"},
                                DataType =
                                    new WParameterizedDataTypeReference
                                    {
                                        Name = new WSchemaObjectName(new Identifier {Value = "bigint"})
                                    },
                                Constraints = new List<WConstraintDefinition>
                                {
                                    new WUniqueConstraintDefinition
                                    {
                                        Clustered = true,
                                        IsPrimaryKey = true
                                    }
                                }
                            },
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "srcCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier {Value = "int"}),
                                },
                            },
                        },
                        Indexes = new List<WIndexDefinition>(),
                        TableConstraints = new List<WConstraintDefinition>()
                    }
                });
                

                var tableVar = new WSpecialNamedTableReference
                {
                    TableObjectName = new WSchemaObjectName(new Identifier { Value = "@" + insertEdgeTempTable+"_1" })
                };
                var reversedTableVar = new WSpecialNamedTableReference
                {
                    TableObjectName = new WSchemaObjectName(new Identifier { Value = "@" + insertEdgeTempTable + "_2" })
                };
                WInsertSpecification[] insertStat = new WInsertSpecification[]
                {
                    new WInsertSpecification
                    {
                        Target = tableVar,
                        InsertSource = new WSelectInsertSource
                        {
                            Select = edgeSelectQuery
                        }
                    },
                    new WInsertSpecification
                    {
                        Target = reversedTableVar,
                        InsertSource = new WSelectInsertSource
                        {
                            Select = reversedEdgeSelectQuery
                        }
                    }
                };

                // Update statments for edge and reversed edge
                WUpdateSpecification[] updateStat = new WUpdateSpecification[]
                {
                    new WUpdateSpecification
                    {
                        Target = node.Target,
                        SetClauses = new List<WSetClause> {setClauses[0], setClauses[2]},
                        FromClause = new WFromClause
                        {
                            TableReferences = new WTableReference[]
                            {
                                new WQueryDerivedTable
                                {
                                    Alias = new Identifier {Value = insertEdgeTableName},
                                    QueryExpr = new WSelectQueryBlock
                                    {
                                        SelectElements = new List<WSelectElement>()
                                        {
                                            new WSelectStarExpression()
                                        },
                                        FromClause = new WFromClause
                                        {
                                            TableReferences = new List<WTableReference> {tableVar}
                                        }
                                    }
                                },
                            }
                        },
                        WhereClause = whereClause,
                        TopRowFilter = node.TopRowFilter
                    },
                    new WUpdateSpecification
                    {
                        Target = new WNamedTableReference
                        {
                            TableObjectName = sinkNameTable.TableObjectName
                        },
                        SetClauses = new List<WSetClause> {setClauses[1]},
                        FromClause = new WFromClause
                        {
                            TableReferences = new WTableReference[]
                            {
                                new WQueryDerivedTable
                                {
                                    Alias = new Identifier {Value = insertEdgeTableName},
                                    QueryExpr = new WSelectQueryBlock
                                    {
                                        SelectElements = new List<WSelectElement>()
                                        {
                                            new WSelectStarExpression()
                                        },
                                        FromClause = new WFromClause
                                        {
                                            TableReferences = new List<WTableReference> {reversedTableVar}
                                        }
                                    }
                                },
                            }
                        },
                        WhereClause = whereClause,
                        TopRowFilter = node.TopRowFilter
                    },

                };
                bool sourceFirst = compare<0;
                if (sourceFirst)
                {

                    // With expression
                    res.Add(insertCommonTableExpr);
                    res.Add(insertStat[0]);
                    res.Add(updateStat[0]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_1", insertEdgeTempTable)
                    });

                    // With expression
                    res.Add(insertCommonTableExpr);
                    res.Add(insertStat[1]);
                    res.Add(updateStat[1]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_2", insertEdgeTempTable)
                    });
                }
                else
                {

                    // With expression
                    res.Add(insertCommonTableExpr);
                    res.Add(insertStat[1]);
                    res.Add(updateStat[1]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_2", insertEdgeTempTable)
                    });

                    // With expression
                    res.Add(insertCommonTableExpr);
                    res.Add(insertStat[0]);
                    res.Add(updateStat[0]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_1", insertEdgeTempTable)
                    });
                }

            }
            //res.Add(new WSqlUnknownStatement { Statement = "DROP TABLE " + insertEdgeTempTable });

        }

        private void TranslateEdgeDelete(WDeleteEdgeSpecification node, List<WSqlStatement> res)
        {
            const string deleteEdgeTableName = "GraphView_DeleteEdgeInternalTable";
            string deleteEdgeTempTable = "DeleteTmp_" + Path.GetRandomFileName().Replace(".", "").Substring(0, 8);

            // Wraps the select statement in a common table expression
            WTableReference sinkTable, sourceTable;
            var deleteCommonTableExpr = ConstructDeleteEdgeSelect(node.SelectDeleteExpr,
                node.EdgeColumn, deleteEdgeTempTable,
                out sinkTable, out sourceTable);
            var sourceNameTable = sourceTable as WNamedTableReference;
            if (sourceNameTable == null)
                throw new GraphViewException("Source table of DELETE EDGE statement should be a named table reference.");
            var sinkNameTable = sinkTable as WNamedTableReference;
            if (sinkNameTable == null)
                throw new GraphViewException("Sink table of DELETE EDGE statement should be a named table reference.");


            // Get edge select query and reversed edge select query
            WSelectQueryBlock edgeSelectQuery = new WSelectQueryBlock
            {
                SelectElements = new List<WSelectElement>
                {
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "src"})
                        },
                        ColumnName = "source"
                    },
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WFunctionCall
                        {
                            CallTarget = new WMultiPartIdentifierCallTarget
                            {
                                Identifiers = new WMultiPartIdentifier(new Identifier {Value = "dbo"})
                            },
                            FunctionName = new Identifier {Value = GraphViewConnection.GraphViewUdfAssemblyName + "EdgeIdEncoder"},
                            Parameters = new List<WScalarExpression>
                            {
                                new WColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "edgeid"})
                                },
                            },
                        },
                        ColumnName = "binary1"
                    },
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WFunctionCall
                        {
                            FunctionName = new Identifier{Value = "COUNT"},
                            Parameters = new List<WScalarExpression>
                            {
                                new WColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "sink"})
                                },
                            },
                        },
                        ColumnName = "sinkCnt"
                    }
                    
                },
                FromClause = new WFromClause
                {
                    TableReferences = new List<WTableReference>
                    {
                        new WSpecialNamedTableReference
                        {
                            TableObjectName = new WSchemaObjectName(new Identifier{Value = deleteEdgeTempTable })
                        }
                    }
                },
                GroupByClause = new WGroupByClause
                {
                    GroupingSpecifications = new List<WGroupingSpecification>
                    {
                        new WExpressionGroupingSpec
                        {
                            Expression = new WValueExpression("src",false)
                        }
                    }
                }
            };
            WSelectQueryBlock reversedEdgeSelectQuery = new WSelectQueryBlock
            {
                SelectElements = new List<WSelectElement>
                {
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "sink"})
                        },
                        ColumnName = "source"
                    },
                    new WSelectScalarExpression
                    {
                        SelectExpr = new WFunctionCall
                        {
                            FunctionName =
                                new Identifier
                                {
                                    Value = "COUNT"
                                },
                            Parameters = new List<WScalarExpression>
                            {
                                new WColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "src"})
                                },
                            }
                        },
                        ColumnName = "srcCnt"
                    },

                },
                FromClause = edgeSelectQuery.FromClause,
                GroupByClause = new WGroupByClause
                {
                    GroupingSpecifications = new List<WGroupingSpecification>
                    {
                        new WExpressionGroupingSpec
                        {
                            Expression = new WValueExpression("sink", false)
                        }
                    }
                }
            };

            //Check src & sink
            string sourceTableName = sourceNameTable.TableObjectName.ToString();
            string sinkTableName = sinkNameTable.TableObjectName.ToString();
            int compare = string.CompareOrdinal(sourceTableName, sinkTableName);
            WSetClause[] setClauses = new WSetClause[]
            {
                new WFunctionCallSetClause()
                {
                    MutatorFuction = new WFunctionCall
                    {
                        CallTarget = new WMultiPartIdentifierCallTarget()
                        {
                            Identifiers =
                                new WMultiPartIdentifier(new Identifier
                                {
                                    Value = node.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + "DeleteCol"
                                })
                        },
                        FunctionName = new Identifier {Value = "WRITE"},
                        Parameters = new WScalarExpression[]
                        {
                            new WColumnReferenceExpression
                            {
                                MultiPartIdentifier = new WMultiPartIdentifier(
                                    new Identifier {Value = deleteEdgeTableName},
                                    new Identifier {Value = "binary1"})
                            },
                            new WValueExpression("null", false),
                            new WValueExpression("null", false)
                        }
                    }
                },
                new WAssignmentSetClause()
                {
                    AssignmentKind = AssignmentKind.Equals,
                    Column = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(new Identifier
                            {
                                Value = "InDegree"
                            })
                    },
                    NewValue = new WBinaryExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(new Identifier
                                {
                                    Value = "InDegree"
                                })
                        },
                        SecondExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "srcCnt"})
                        },
                        ExpressionType = BinaryExpressionType.Subtract
                    }
                },
                new WAssignmentSetClause()
                {
                    AssignmentKind = AssignmentKind.Equals,
                    Column = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(new Identifier
                            {
                                Value = node.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + "OutDegree"
                            })
                    },
                    NewValue = new WBinaryExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(new Identifier
                                {
                                    Value = node.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + "OutDegree"
                                })
                        },
                        SecondExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "sinkCnt"})
                        },
                        ExpressionType = BinaryExpressionType.Subtract
                    }
                }
                
            };
            WWhereClause whereClause = new WWhereClause
            {
                SearchCondition = new WBooleanComparisonExpression
                {
                    FirstExpr = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier = new WMultiPartIdentifier(new Identifier { Value = "GlobalNodeId" })
                    },
                    SecondExpr = new WColumnReferenceExpression
                    {
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier { Value = deleteEdgeTableName },
                            new Identifier { Value = "source" }
                            )
                    },
                    ComparisonType = BooleanComparisonType.Equals
                }
            };
            if (compare == 0)
            {
                res.Add(new WDeclareTableVariable
                {
                    VariableName = new Identifier { Value = "@" + deleteEdgeTempTable},
                    Definition = new WTableDefinition
                    {
                        ColumnDefinitions = new WColumnDefinition[]
                        {
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "source"},
                                DataType = new WParameterizedDataTypeReference{Name = new WSchemaObjectName(new Identifier{Value = "bigint"})},
                                Constraints = new List<WConstraintDefinition>
                                {
                                    new WUniqueConstraintDefinition
                                    {
                                        Clustered = true,
                                        IsPrimaryKey = true
                                    }
                                }
                            }, 
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "binary1"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier{Value = "varbinary"}),
                                    Parameters = new List<Literal>{new MaxLiteral{Value = "max"}}
                                },
                            }, 
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "sinkCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier{Value = "int"}),
                                },
                            }, 
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier{Value = "srcCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier{Value = "int"}),
                                },
                            }, 
                        },
                        Indexes = new List<WIndexDefinition>(),
                        TableConstraints = new List<WConstraintDefinition>()
                    }
                });
                var tableVar = new WSpecialNamedTableReference
                {
                    TableObjectName = new WSchemaObjectName(new Identifier { Value = "@" + deleteEdgeTempTable })
                };
                res.Add(deleteCommonTableExpr);
                res.Add(new WInsertSpecification
                {
                    Target = tableVar,
                    InsertSource = new WSelectInsertSource
                    {
                        Select = new WSelectQueryBlock
                        {
                            SelectElements = new WSelectElement[]
                            {
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WFunctionCall
                                    {
                                        FunctionName = new Identifier {Value = "ISNULL"},
                                        Parameters = new WScalarExpression[]
                                        {
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = deleteEdgeTableName + "_1"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            },
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = deleteEdgeTableName + "_2"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            },
                                        }
                                    },
                                    ColumnName = "source"
                                },
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WColumnReferenceExpression
                                    {
                                        MultiPartIdentifier =
                                            new WSchemaObjectName(new Identifier[] {new Identifier {Value = "binary1"}}),
                                    }
                                },
                                //new WSelectScalarExpression
                                //{
                                //    SelectExpr = new WColumnReferenceExpression
                                //    {
                                //                MultiPartIdentifier = new WSchemaObjectName(new Identifier[]{new Identifier{Value = "binary2"}}),
                                //    }
                                //}, 
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WFunctionCall
                                    {
                                        FunctionName = new Identifier {Value = "ISNULL"},
                                        Parameters = new WScalarExpression[]
                                        {
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = deleteEdgeTableName + "_1"},
                                                        new Identifier {Value = "sinkCnt"}
                                                    }),
                                            },
                                            new WValueExpression
                                            {
                                                Value = "0"
                                            },
                                        }
                                    },
                                    ColumnName = "sinkCnt"
                                },
                                new WSelectScalarExpression
                                {
                                    SelectExpr = new WFunctionCall
                                    {
                                        FunctionName = new Identifier {Value = "ISNULL"},
                                        Parameters = new WScalarExpression[]
                                        {
                                            new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = deleteEdgeTableName + "_2"},
                                                        new Identifier {Value = "srcCnt"}
                                                    }),
                                            },
                                            new WValueExpression
                                            {
                                                Value = "0"
                                            },
                                        }
                                    },
                                    ColumnName = "srcCnt"
                                },
                            },
                            FromClause = new WFromClause
                            {
                                TableReferences = new WTableReference[]
                                {
                                    new WQualifiedJoin
                                    {
                                        QualifiedJoinType = QualifiedJoinType.FullOuter,
                                        FirstTableRef = new WQueryDerivedTable
                                        {
                                            Alias = new Identifier {Value = deleteEdgeTableName + "_1"},
                                            QueryExpr = edgeSelectQuery
                                        },
                                        SecondTableRef = new WQueryDerivedTable
                                        {
                                            Alias = new Identifier {Value = deleteEdgeTableName + "_2"},
                                            QueryExpr = reversedEdgeSelectQuery
                                        },
                                        JoinCondition = new WBooleanComparisonExpression()
                                        {
                                            ComparisonType = BooleanComparisonType.Equals,
                                            FirstExpr = new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = deleteEdgeTableName + "_1"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            },
                                            SecondExpr = new WColumnReferenceExpression
                                            {
                                                MultiPartIdentifier =
                                                    new WSchemaObjectName(new Identifier[]
                                                    {
                                                        new Identifier {Value = deleteEdgeTableName + "_2"},
                                                        new Identifier {Value = "source"}
                                                    }),
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    }
                });
                res.Add(new WUpdateSpecification
                {
                    Target = new WNamedTableReference
                    {
                        TableObjectName = sourceNameTable.TableObjectName
                    },
                    SetClauses = setClauses,
                    FromClause = new WFromClause
                    {
                        TableReferences = new WTableReference[]
                        {
                            new WQueryDerivedTable
                            {
                                Alias = new Identifier {Value = deleteEdgeTableName},
                                QueryExpr = new WSelectQueryBlock
                                {
                                    SelectElements = new List<WSelectElement>()
                                    {
                                        new WSelectStarExpression()
                                    },
                                    FromClause = new WFromClause
                                    {
                                        TableReferences = new List<WTableReference> {tableVar}
                                    }
                                }
                            },
                        }
                    },
                    WhereClause = whereClause,
                    TopRowFilter = node.TopRowFilter,
                });
                res.Add(new WSqlUnknownStatement
                {
                    Statement = String.Format("DELETE FROM @{0}", deleteEdgeTempTable)
                });
            }
            else
            {
                // Declare table variables
                res.Add(new WDeclareTableVariable
                {
                    VariableName = new Identifier { Value = "@" + deleteEdgeTempTable + "_1" },
                    Definition = new WTableDefinition
                    {
                        ColumnDefinitions = new WColumnDefinition[]
                        {
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "source"},
                                DataType =
                                    new WParameterizedDataTypeReference
                                    {
                                        Name = new WSchemaObjectName(new Identifier {Value = "bigint"})
                                    },
                                Constraints = new List<WConstraintDefinition>
                                {
                                    new WUniqueConstraintDefinition
                                    {
                                        Clustered = true,
                                        IsPrimaryKey = true
                                    }
                                }
                            },
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "binary1"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier {Value = "varbinary"}),
                                    Parameters = new List<Literal> {new MaxLiteral {Value = "max"}}
                                },
                            },
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "sinkCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier {Value = "int"}),
                                },
                            },
                        },
                        Indexes = new List<WIndexDefinition>(),
                        TableConstraints = new List<WConstraintDefinition>()
                    }
                });
                res.Add(new WDeclareTableVariable
                {
                    VariableName = new Identifier { Value = "@" + deleteEdgeTempTable + "_2" },
                    Definition = new WTableDefinition
                    {
                        ColumnDefinitions = new WColumnDefinition[]
                        {
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "source"},
                                DataType =
                                    new WParameterizedDataTypeReference
                                    {
                                        Name = new WSchemaObjectName(new Identifier {Value = "bigint"})
                                    },
                                Constraints = new List<WConstraintDefinition>
                                {
                                    new WUniqueConstraintDefinition
                                    {
                                        Clustered = true,
                                        IsPrimaryKey = true
                                    }
                                }
                            },
                            new WColumnDefinition
                            {
                                ColumnIdentifier = new Identifier {Value = "srcCnt"},
                                DataType = new WParameterizedDataTypeReference
                                {
                                    Name = new WSchemaObjectName(new Identifier {Value = "int"}),
                                },
                            },
                        },
                        Indexes = new List<WIndexDefinition>(),
                        TableConstraints = new List<WConstraintDefinition>()
                    }
                });

                var tableVar = new WSpecialNamedTableReference
                {
                    TableObjectName = new WSchemaObjectName(new Identifier { Value = "@" + deleteEdgeTempTable + "_1" })
                };
                var reversedTableVar = new WSpecialNamedTableReference
                {
                    TableObjectName = new WSchemaObjectName(new Identifier { Value = "@" + deleteEdgeTempTable + "_2" })
                };
                WInsertSpecification[] insertStat = new WInsertSpecification[]
                {
                    new WInsertSpecification
                    {
                        Target = tableVar,
                        InsertSource = new WSelectInsertSource
                        {
                            Select = edgeSelectQuery
                        }
                    },
                    new WInsertSpecification
                    {
                        Target = reversedTableVar,
                        InsertSource = new WSelectInsertSource
                        {
                            Select = reversedEdgeSelectQuery
                        }
                    }
                };

                // Update statments for edge and reversed edge
                WUpdateSpecification[] updateStat = new WUpdateSpecification[]
                {
                    new WUpdateSpecification
                    {
                        Target = new WNamedTableReference
                        {
                            TableObjectName = sourceNameTable.TableObjectName
                        },
                        SetClauses = new List<WSetClause> {setClauses[0], setClauses[2]},
                        FromClause = new WFromClause
                        {
                            TableReferences = new WTableReference[]
                            {
                                new WQueryDerivedTable
                                {
                                    QueryExpr = new WSelectQueryBlock
                                    {
                                        SelectElements = new List<WSelectElement>()
                                        {
                                            new WSelectStarExpression()
                                        },
                                        FromClause = new WFromClause
                                        {
                                            TableReferences = new List<WTableReference> {tableVar}
                                        }
                                    },
                                    Alias = new Identifier {Value = deleteEdgeTableName},

                                },
                            }
                        },
                        WhereClause = whereClause,
                        TopRowFilter = node.TopRowFilter
                    },
                    new WUpdateSpecification
                    {
                        Target = new WNamedTableReference
                        {
                            TableObjectName = sinkNameTable.TableObjectName
                        },
                        SetClauses = new List<WSetClause> {setClauses[1]},
                        FromClause = new WFromClause
                        {
                            TableReferences = new WTableReference[]
                            {
                                new WQueryDerivedTable
                                {
                                    Alias = new Identifier {Value = deleteEdgeTableName},
                                    QueryExpr = new WSelectQueryBlock
                                    {
                                        SelectElements = new List<WSelectElement>()
                                        {
                                            new WSelectStarExpression()
                                        },
                                        FromClause = new WFromClause
                                        {
                                            TableReferences = new List<WTableReference> {reversedTableVar}
                                        }
                                    }
                                },
                            }
                        },
                        WhereClause = whereClause,
                        TopRowFilter = node.TopRowFilter
                    },

                };
                bool sourceFirst = compare < 0;
                if (sourceFirst)
                {

                    // With expression
                    res.Add(deleteCommonTableExpr);
                    res.Add(insertStat[0]);
                    res.Add(updateStat[0]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_1", deleteEdgeTempTable)
                    });

                    // With expression
                    res.Add(deleteCommonTableExpr);
                    res.Add(insertStat[1]);
                    res.Add(updateStat[1]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_2", deleteEdgeTempTable)
                    });
                }
                else
                {

                    // With expression
                    res.Add(deleteCommonTableExpr);
                    res.Add(insertStat[1]);
                    res.Add(updateStat[1]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_2", deleteEdgeTempTable)
                    });

                    // With expression
                    res.Add(deleteCommonTableExpr);
                    res.Add(insertStat[0]);
                    res.Add(updateStat[0]);
                    res.Add(new WSqlUnknownStatement
                    {
                        Statement = String.Format("DELETE FROM @{0}_1", deleteEdgeTempTable)
                    });
                }

            }

        }

        private WCommonTableExpression ConstructDeleteEdgeSelect(WSelectQueryBlock node, WEdgeColumnReferenceExpression edgeCol, string tempTableName
            ,out WTableReference sinkTable, out WTableReference sourceTable)
        {
            sourceTable = null;
            sinkTable = null;
            string sourceTableName = null;
            string sinkTableName = null;
            var sourceTableScalar = node.SelectElements[0] as WSelectScalarExpression;
            if (sourceTableScalar == null)
                return null;
            var sourceTableColumn = sourceTableScalar.SelectExpr as WColumnReferenceExpression;
            if (sourceTableColumn == null)
                return null;
            sourceTableName = sourceTableColumn.MultiPartIdentifier.Identifiers.Last().Value;

            var sourceNodeIdExpr = new WColumnReferenceExpression();
            foreach (var identifier in sourceTableColumn.MultiPartIdentifier.Identifiers)
                sourceNodeIdExpr.Add(identifier);
            sourceNodeIdExpr.AddIdentifier("GlobalNodeId");

            var collectVarVisitor = new CollectVariableVisitor();
            var context = collectVarVisitor.Invoke(node);
            if (!context.NodeTableDictionary.Any())
            {
                throw new GraphViewException("Missing From Clause in Delete Edge statement");
            }
            WColumnReferenceExpression sinkNodeIdExpr = null;
            sourceTable = context[sourceTableName];

            var groupByParams = new List<WScalarExpression>();
            for (var index = 1; index < node.SelectElements.Count; ++index)
            {
                var element = node.SelectElements[index] as WSelectScalarExpression;
                if (element == null)
                    return null;
                //sink table
                if (index == 1)
                {
                    var sinkTableColumn = element.SelectExpr as WColumnReferenceExpression;
                    if (sinkTableColumn == null)
                        return null;

                    sinkTableName = sinkTableColumn.MultiPartIdentifier.Identifiers.Last().Value;
                    sinkTable = context[sinkTableName];

                    sinkNodeIdExpr = new WColumnReferenceExpression();
                    foreach (var identifier in sinkTableColumn.MultiPartIdentifier.Identifiers)
                        sinkNodeIdExpr.Add(identifier);
                    sinkNodeIdExpr.AddIdentifier("GlobalNodeId");
                }
            }

            var sourceNodeId = new WSelectScalarExpression
            {
                SelectExpr = sourceNodeIdExpr,
                ColumnName = "src",
            };
            var sinkNodeId = new WSelectScalarExpression
            {
                SelectExpr = sinkNodeIdExpr,
                ColumnName = "sink"
            };
            var edgeId = new WSelectScalarExpression
            {
                SelectExpr = new WColumnReferenceExpression
                {
                    MultiPartIdentifier = new WMultiPartIdentifier(
                        new Identifier[]
                        {
                            new Identifier {Value = edgeCol.Alias??string.Format("{0}_{1}_{2}",sourceTableName,edgeCol.MultiPartIdentifier.Identifiers.Last().Value,sinkTableName)},
                            new Identifier {Value = "Edgeid"}
                        }
                        )
                },
                ColumnName = "edgeid"
            };

            var elements = new List<WSelectElement>
            {
                sourceNodeId,
                sinkNodeId,
                edgeId
            };

            return new WCommonTableExpression
            {
                ExpressionName = new Identifier { Value = tempTableName },
                QueryExpression = new WSelectQueryBlock
                {
                    FromClause = node.FromClause,
                    WhereClause = new WWhereClause
                    {
                        SearchCondition = node.WhereClause.SearchCondition
                    },
                    HavingClause = node.HavingClause,
                    OrderByClause = node.OrderByClause,
                    TopRowFilter = node.TopRowFilter,
                    UniqueRowFilter = node.UniqueRowFilter,
                    SelectElements = elements,
                    MatchClause = node.MatchClause,
                }

            };
        }

        private void TranslateNodeDelete(WDeleteNodeSpecification node, List<WSqlStatement> res)
        {
            var table = node.Target as WNamedTableReference;
            if (table == null)
                throw new GraphViewException("Target of DELETE NODE statement should be a named table reference.");
            WBooleanExpression cond = new WBooleanComparisonExpression
            {
                ComparisonType = BooleanComparisonType.Equals,
                FirstExpr = new WColumnReferenceExpression
                {
                    MultiPartIdentifier = new WMultiPartIdentifier(new Identifier { Value = "InDegree" })
                },
                SecondExpr = new WValueExpression
                {
                    Value = "0"
                }
            };
            var tableSchema = table.TableObjectName.SchemaIdentifier == null
                ? "dbo"
                : table.TableObjectName.SchemaIdentifier.Value;
            var tableName = table.TableObjectName.BaseIdentifier.Value;
            using (var command = Tx.Connection.CreateCommand())
            {
                command.Transaction = Tx;
                command.CommandText = string.Format(
                    @"SELECT [TableSchema], [TableName], [ColumnName] FROM [{3}] WHERE TableSchema = '{0}' and [TableName]='{1}' and ColumnRole={2}",
                    tableSchema, tableName, 1, GraphViewConnection.MetadataTables[1]);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var col = reader["ColumnName"].ToString();
                        cond = WBooleanBinaryExpression.Conjunction(cond, new WBooleanComparisonExpression
                        {
                            ComparisonType = BooleanComparisonType.Equals,
                            FirstExpr = new WColumnReferenceExpression
                            {
                                MultiPartIdentifier = new WMultiPartIdentifier(new Identifier { Value = col + "OutDegree" })
                            },
                            SecondExpr = new WValueExpression
                            {
                                Value = "0"
                            }

                        });
                    }

                }
            }
            WWhereClause checkDeleteNode = new WWhereClause
            {
                SearchCondition = new WBooleanNotExpression { Expression = cond }
            };
            if (node.WhereClause == null ||
                node.WhereClause.SearchCondition == null)
            {
                node.WhereClause = new WWhereClause { SearchCondition = cond };
            }
            else
            {
                checkDeleteNode = new WWhereClause
                {
                    SearchCondition = new WBooleanBinaryExpression
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.And,
                        FirstExpr = node.WhereClause.SearchCondition,
                        SecondExpr = checkDeleteNode.SearchCondition
                    }
                };
                node.WhereClause = new WWhereClause
                {
                    SearchCondition = new WBooleanBinaryExpression
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.And,
                        FirstExpr = node.WhereClause.SearchCondition,
                        SecondExpr = cond
                    }
                };
            }
            using (var command = Tx.Connection.CreateCommand())
            {
                command.Transaction = Tx;
                command.CommandText = string.Format("SELECT InDegree FROM {0}.{1} {2}", tableSchema, tableName,
                    checkDeleteNode.ToString());
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        throw new GraphViewException(
                            string.Format(
                                "Node(s) of node table {0} being deleted still has/have ingoing or outdoing edge(s)",
                                tableName));
                    }
                }
            }

            res.Add(new WDeleteSpecification
            {
                FromClause = node.FromClause,
                Target = node.Target,
                TopRowFilter = node.TopRowFilter,
                WhereClause = node.WhereClause,
            });
        }
    }


    class InsertEdgeSelectVisitor : WSqlFragmentVisitor
    {
        private WCommonTableExpression _result;
        private WTableReference _sinkTable;
        private List<WScalarExpression> _edgeProperties;
        private string _tempTableName;
        private string _sourceTableName;

        public WCommonTableExpression Invoke(WSqlFragment node, string tempTableName, string sourceTableName,
            out WTableReference sinkTable, out List<WScalarExpression> edgeProperties)
        {
            _edgeProperties = new List<WScalarExpression>();
            _tempTableName = tempTableName;
            _sourceTableName = sourceTableName;
            node.Accept(this);
            sinkTable = _sinkTable;
            _edgeProperties.Insert(0,
                new WColumnReferenceExpression
                {
                    MultiPartIdentifier = new WMultiPartIdentifier(new Identifier {Value = "sink"})
                }
                );
            edgeProperties = _edgeProperties;
            return _result;
        }

        //public override void Visit(WQueryParenthesisExpression node)
        //{
        //    node.QueryExpr.Accept(this);
        //    _result = new WQueryParenthesisExpression
        //    {
        //        OrderByClause = node.OrderByClause,
        //        QueryExpr = _result
        //    };
        //}

        public override void Visit(WBinaryQueryExpression node)
        {
            throw new GraphViewException("Binary query expression is not allowed in INSERT EDGE statement.");
        }

        public override void Visit(WSelectQueryBlock node)
        {
            if (node.SelectElements == null || node.SelectElements.Count < 2)
            {
                throw new GraphViewException("Numbers of select elements mismatch.");
            }
            if (node.GroupByClause != null)
            {
                throw new GraphViewException("GROUP BY clause is not allowed in INSERT EDGE statement.");
            }
            var sourceTableScalar = node.SelectElements[0] as WSelectScalarExpression;
            if (sourceTableScalar == null)
                throw new GraphViewException("Source node id should be a scalar expression.");
            var sourceTableColumn = sourceTableScalar.SelectExpr as WColumnReferenceExpression;
            if (sourceTableColumn == null)
                throw new GraphViewException("Source node id column should be a column reference expression.");
            var sourceTableAlias = sourceTableColumn.MultiPartIdentifier.Identifiers.Last().Value;
            
            var collectVarVisitor = new CollectVariableVisitor();
            var context = collectVarVisitor.Invoke(node);
            var sourceTable = context[sourceTableAlias] as WNamedTableReference;
            if (sourceTable == null)
            {
                throw new GraphViewException("Source table of INSERT EDGE statement should be a named table reference.");
            }
            if (
                string.Compare(_sourceTableName, sourceTable.TableObjectName.BaseIdentifier.Value,
                    StringComparison.OrdinalIgnoreCase) != 0)
                throw new GraphViewException("Source node table in the SELECT is mismatched with the INSERT source");


            var sourceNodeIdExpr = new WColumnReferenceExpression();
            foreach (var identifier in sourceTableColumn.MultiPartIdentifier.Identifiers)
                sourceNodeIdExpr.Add(identifier);
            sourceNodeIdExpr.AddIdentifier("GlobalNodeId");

            //if (node.SelectElements.Count < 2)
            //    throw new GraphViewException("Source and Sink tables should be specified in select elements.");

            WColumnReferenceExpression sinkNodeIdExpr = null;
            for (var index = 1; index < node.SelectElements.Count; ++index)
            {
                var element = node.SelectElements[index] as WSelectScalarExpression;
                if (element == null)
                    throw new GraphViewException("Edge property should be a scalar expression.");
                //sink table
                if (index == 1)
                {
                    var sinkTableColumn = element.SelectExpr as WColumnReferenceExpression;
                    if (sinkTableColumn == null)
                        throw new GraphViewException("Sink node id column should be a column reference expression.");

                    var sinkTableAlias = sinkTableColumn.MultiPartIdentifier.Identifiers.Last().Value;
                    _sinkTable = context[sinkTableAlias];

                    sinkNodeIdExpr = new WColumnReferenceExpression();
                    foreach (var identifier in sinkTableColumn.MultiPartIdentifier.Identifiers)
                        sinkNodeIdExpr.Add(identifier);
                    sinkNodeIdExpr.AddIdentifier("GlobalNodeId");
                }
                else
                {
                    _edgeProperties.Add(element.SelectExpr);
                }

            }

            var sourceNodeId = new WSelectScalarExpression
            {
                SelectExpr = sourceNodeIdExpr,
                ColumnName = "src",
            };
            var sinkNodeId = new WSelectScalarExpression
            {
                SelectExpr = sinkNodeIdExpr,
                ColumnName = "sink"
            };


            var elements = new List<WSelectElement>
            {
                sourceNodeId,
                sinkNodeId
            };

            var result = new WCommonTableExpression
            {
                ExpressionName = new Identifier {Value = _tempTableName},
                QueryExpression = new WSelectQueryBlock
                {
                    FromClause = node.FromClause,
                    WhereClause = new WWhereClause
                    {
                        SearchCondition = node.WhereClause.SearchCondition
                    },
                    HavingClause = node.HavingClause,
                    OrderByClause = node.OrderByClause,
                    TopRowFilter = node.TopRowFilter,
                    UniqueRowFilter = node.UniqueRowFilter,
                    SelectElements = elements,
                    MatchClause = node.MatchClause
                }
            };
            

            _result = result;
        }
    }
}