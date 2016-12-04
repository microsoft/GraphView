using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinToSqlContext
    {
        public GremlinVariable RootVariable { get; set; }
        public bool FromOuter;


        public GremlinToSqlContext()
        {
            RemainingVariableList = new List<GremlinVariable>();
            //VariablePredicates = new Dictionary<GremlinVariable, WBooleanExpression>();
            //CrossVariableConditions = new List<WBooleanExpression>();
            Predicates = null;
            ProjectionList = new List<Projection>();
            Paths = new List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>();
            AliasToGremlinVariableList = new Dictionary<string, List<GremlinVariable>>();
            //GroupByVariable = new Tuple<GremlinVariable, GroupByRecord>();
            //OrderByVariable = new Tuple<GremlinVariable, OrderByRecord>();
            Statements = new List<WSqlStatement>();
        }
        /// <summary>
        /// A list of Gremlin variables. The variables are expected to 
        /// follow the (vertex)-(edge|path)-(vertex)-... pattern
        /// </summary>
        public List<GremlinVariable> RemainingVariableList { get; set; }

        /// <summary>
        /// A collection of variables and their predicates
        /// </summary>
        //public Dictionary<GremlinVariable, WBooleanExpression> VariablePredicates { get; set; }

        /// <summary>
        /// A list of boolean expressions, each of which is on multiple variables
        /// </summary>
        //public List<WBooleanExpression> CrossVariableConditions { get; set; }

        public WBooleanExpression Predicates;

        /// <summary>
        /// The variable on which the new traversal operates
        /// </summary>
        public GremlinVariable CurrVariable;

        public Dictionary<string, List<GremlinVariable>> AliasToGremlinVariableList;

        /// <summary>
        /// A list of Gremlin variables and their properties the query projects. 
        /// When no property is specified, the variable projects its "ID":
        /// If the variable is a vertex variable, it projects the vertex ID;
        /// If the variable is an edge variable, it projects the (source vertex ID, sink vertex ID, offset) pair. 
        /// 
        /// The projection is updated, as it is passed through every traversal. 
        /// </summary> 
        public List<Projection> ProjectionList { get; set; }

        public List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>> Paths { get; set; }
        /// <summary>
        /// The Gremlin variable and its property by which the query groups
        /// </summary>
        public Tuple<GremlinVariable, GroupByRecord> GroupByVariable { get; set; }

        /// <summary>
        /// The Gremlin variable and its property by which the query orders
        /// </summary>
        public Tuple<GremlinVariable, OrderByRecord> OrderByVariable { get; set; }

        public List<WSqlStatement> Statements;

        public void AddNewVariable(GremlinVariable gremlinVar, List<string> labels)
        {
            RemainingVariableList.Add(gremlinVar);
            if (labels.Count == 0) return;
            foreach (var label in labels)
            {
                AddAliasToGremlinVariable(label, gremlinVar);
            }
        }

        public void AddAliasToGremlinVariable(string label, GremlinVariable gremlinVar)
        {
            if (!AliasToGremlinVariableList.ContainsKey(label))
            {
                AliasToGremlinVariableList[label] = new List<GremlinVariable>();
            }
            AliasToGremlinVariableList[label].Add(gremlinVar);
        }

        public void SetCurrVariable(GremlinVariable gremlinVar)
        {
            if (IsSpecCurrVar())
            {
                Statements.Add(ToSetVariableStatement());
            }
            CurrVariable = gremlinVar;
        }

        public bool IsSpecCurrVar()
        {
            if (CurrVariable is GremlinAddEVariable) return true;
            if (CurrVariable is GremlinAddVVariable) return true;
            if (CurrVariable is GremlinDerivedVariable) return true;
            return false;
        }

        //Projection
        public void SetDefaultProjection(GremlinVariable newGremlinVar)
        {
            ProjectionList.Clear();
            ProjectionList.Add(new ColumnProjection(newGremlinVar, "id"));
        }

        public void SetCurrProjection(params Projection[] projections)
        {
            ProjectionList.Clear();
            foreach (var projection in projections)
            {
                ProjectionList.Add(projection);
            }
        }

        public void SetCurrProjection(List<Projection> projections)
        {
            ProjectionList = projections;
        }

        public void SetStarProjection()
        {
            ProjectionList.Clear();
            ProjectionList.Add(new StarProjection());
        }

        //------

        public WBooleanExpression ToSqlBoolean()
        {
            if (RemainingVariableList.Count == 0)
            {
                return Predicates;
            }
            else
            {
                WSqlStatement subQueryExpr = ToSelectQueryBlock();
                return GremlinUtil.GetExistPredicate(subQueryExpr);
            }
        }

        public WScalarExpression ToSqlScalar()
        {
            return null;
        }

        public WSqlScript ToSqlScript()
        {
            WSqlScript script = new WSqlScript()
            {
                Batches = new List<WSqlBatch>()
            };
            List<WSqlBatch> batchList = GetBatchList();
            script.Batches = batchList;
            return script;
        }

        public List<WSqlBatch> GetBatchList()
        {
            List<WSqlBatch> batchList = new List<WSqlBatch>();
            WSqlBatch batch = new WSqlBatch()
            {
                Statements = new List<WSqlStatement>()
            };
            batch.Statements = GetStatements();
            batchList.Add(batch);
            return batchList;
        }

        public List<WSqlStatement> GetStatements()
        {
            Statements.Add(ToSetVariableStatement());
            return Statements;
        }

        public WSqlStatement ToSetVariableStatement()
        {
            return GremlinUtil.GetSetVariableStatement(CurrVariable.VariableName, ToSqlStatement());
        }

        public WSqlStatement ToSqlStatement()
        {
            if (CurrVariable is GremlinAddEVariable)
            {
                return ToAddESqlQuery(CurrVariable as GremlinAddEVariable);
            }
            if (CurrVariable is GremlinAddVVariable)
            {
                return ToAddVSqlQuery(CurrVariable as GremlinAddVVariable);
            }
            //if (CurrVariable is GremlinDerivedVariable)
            //{
            //    if ((CurrVariable as GremlinDerivedVariable).Type == GremlinDerivedVariable.DerivedType.UNION)
            //    {
            //        WSetVariableStatement statement = GremlinUtil.GetSetVariableStatement(CurrVariable.VariableName, (CurrVariable as GremlinDerivedVariable).Statement);
            //        return statement;
            //    }
            //}
            else
            {
                return ToSelectQueryBlock();
            }
        }

        public WSelectQueryBlock ToSelectQueryBlock()
        {
            // Construct the new Select Component
            var newSelectElementClause = GetSelectElement();

            //Consturct the new From Cluase;
            var newFromClause = GetFromClause();

            // Construct the new Match Cluase
            var newMatchClause = GetMatchClause();

            // Construct the Where Clause
            var newWhereClause = GetWhereClause();

            // Construct the OrderBy Clause
            var newOrderByClause = GetOrderByClause();
            
            // Construct the GroupBy Clause
            var newGroupByClause = GetGroupByClause();

            // Construct the SelectBlock
            return new WSelectQueryBlock()
            {
                FromClause = newFromClause,
                SelectElements = newSelectElementClause,
                WhereClause = newWhereClause,
                MatchClause = newMatchClause,
                OrderByClause = newOrderByClause,
                GroupByClause = newGroupByClause
            };
        }

        public WSqlStatement ToAddESqlQuery(GremlinAddEVariable currVar)
        {
            var columnK = new List<WColumnReferenceExpression>();

            //var selectBlock = ToSelectQueryBlock() as WSelectQueryBlock; // TODO
            //selectBlock.SelectElements.Clear();
            WSelectQueryBlock selectBlock = new WSelectQueryBlock()
            {
                SelectElements = new List<WSelectElement>(),
                FromClause = new WFromClause()
            };

            selectBlock.FromClause.TableReferences = new List<WTableReference>();
            //from(traversal), so we should add variable in the fromClause, which means the fromVariable is not from outer variable
            if (currVar.IsNewFromVariable)
            {
                var tableReference = GetTableReferenceFromVariable(currVar.FromVariable);
                if (tableReference != null)
                    selectBlock.FromClause.TableReferences.Add(tableReference);
            }
            //to(traversal), so we should add variable in the fromClause, which means the toVariable is not from outer variable
            if (currVar.IsNewToVariable)
            {
                var tableReference = GetTableReferenceFromVariable(currVar.ToVariable);
                if (tableReference != null)
                    selectBlock.FromClause.TableReferences.Add(tableReference);
            }
            

            var fromVarExpr = GremlinUtil.GetColumnReferenceExpression(currVar.FromVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(fromVarExpr));

            var toVarExpr = GremlinUtil.GetColumnReferenceExpression(currVar.ToVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(toVarExpr));

            //Add edge key-value
            columnK.Add(GremlinUtil.GetColumnReferenceExpression("label"));
            var valueExpr = GremlinUtil.GetValueExpression(currVar.EdgeLabel);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(valueExpr));
            foreach (var property in currVar.Properties)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression(property.Key));
                valueExpr = GremlinUtil.GetValueExpression(property.Value.ToString());
                selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(valueExpr));
            }
            
            var insertStatement = new WInsertSpecification()
            {
                Columns = columnK,
                InsertSource = new WSelectInsertSource() { Select = selectBlock },
                Target = GremlinUtil.GetNamedTableReference("Edge")
            };

            return new WInsertEdgeSpecification(insertStatement)
            {
                SelectInsertSource = new WSelectInsertSource() { Select = selectBlock }
            };
        }

        public WSqlStatement ToAddVSqlQuery(GremlinAddVVariable currVar)
        {
            var columnK = new List<WColumnReferenceExpression>();
            var columnV = new List<WScalarExpression>();

            if (currVar.VertexLabel != null)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression("label"));
                columnV.Add(GremlinUtil.GetValueExpression(currVar.VertexLabel));
            }

            foreach (var property in currVar.Properties)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression(property.Key));
                columnV.Add(GremlinUtil.GetValueExpression(property.Value));
            }

            var row = new List<WRowValue>() {new WRowValue() {ColumnValues = columnV}};
            var source = new WValuesInsertSource() {RowValues = row};

            var insertStatement = new WInsertSpecification()
            {
                Columns = columnK,
                InsertSource = source,
                Target = GremlinUtil.GetNamedTableReference("Node")
            };

            return new WInsertNodeSpecification(insertStatement);
        }

        public WFromClause GetFromClause()
        {
            var newFromClause = new WFromClause() { TableReferences = new List<WTableReference>() };
            for (var i = 0; i < RemainingVariableList.Count; i++)
            {
                GremlinVariable currVar = RemainingVariableList[i];
                var tableReference = GetTableReferenceFromVariable(currVar);
                if (tableReference != null)
                    newFromClause.TableReferences.Add(tableReference);
            }
            return newFromClause;
        }

        public WTableReference GetTableReferenceFromVariable(GremlinVariable currVar)
        {
            //if (currVar is GremlinJoinVertexVariable)
            //{
            //    //across apply tvf as v3
            //    GremlinVariable lastVar = RemainingVariableList[i - 1];
            //    newFromClause.TableReferences.RemoveAt(newFromClause.TableReferences.Count - 1);

            //    WUnqualifiedJoin unqualifiedJoin = GremlinUtil.GetUnqualifiedJoin(currVar, lastVar);
            //    newFromClause.TableReferences.Add(unqualifiedJoin);
            //}
            if (currVar is GremlinVertexVariable)
            {
                return GremlinUtil.GetNamedTableReference(currVar);
            }
            //else if (currVar is GremlinScalarVariable)
            //{
            //    GremlinScalarVariable scalarVariable = currVar as GremlinScalarVariable;
            //    WQueryDerivedTable queryDerivedTable = new WQueryDerivedTable()
            //    {
            //        Alias = GremlinUtil.GetIdentifier(scalarVariable.VariableName),
            //        QueryExpr = scalarVariable.ScalarSubquery
            //    };
            //    newFromClause.TableReferences.Add(queryDerivedTable);
            //}
            else if (currVar is GremlinChooseVariable)
            {
                return (currVar as GremlinChooseVariable).ChooseExpr;
            }
            else if (currVar is GremlinCoalesceVariable)
            {
                return (currVar as GremlinCoalesceVariable).CoalesceExpr;
            }
            else if (currVar is GremlinVariableReference)
            {
                return GremlinUtil.GetVariableTableReference((currVar as GremlinVariableReference).Variable.Name);
            }
            else if (currVar is GremlinOptionalVariable)
            {
                return (currVar as GremlinOptionalVariable).OptionalExpr;
            }
            else if (currVar is GremlinSideEffectVariable)
            {
                return (currVar as GremlinSideEffectVariable).SideEffectExpr;
            }
            else if (currVar is GremlinAddVVariable)
            {
                return GremlinUtil.GetVariableTableReference(currVar.VariableName);
            }
            //else if (currVar is GremlinAddEVariable)
            //{
            //    GremlinUtil.GetVariableTableReference(currVar.VariableName);
            //}
            return null;
        }

        //public WTableReference GetDerivedVariableTableReference(GremlinDerivedVariable currVar)
        //{
        //    if (currVar.Type == GremlinDerivedVariable.DerivedType.UNION)
        //    {
        //        return GremlinUtil.GetVariableTableReference(currVar.VariableName);
        //    }
        //}

        public WMatchClause GetMatchClause()
        {
            var newMatchClause = new WMatchClause() { Paths = new List<WMatchPath>() };
            foreach (var path in Paths)
            {
                newMatchClause.Paths.Add(GremlinUtil.GetMatchPath(path));
            }

            foreach (var variable in RemainingVariableList)
            {
                if (variable is GremlinAddEVariable)
                {
                    var addEVar = variable as GremlinAddEVariable;
                    var path = new Tuple<GremlinVariable, GremlinVariable, GremlinVariable>(addEVar.FromVariable,
                        addEVar, addEVar.ToVariable);

                    newMatchClause.Paths.Add(GremlinUtil.GetMatchPath(path));
                }
            }

            return newMatchClause;
        }

        public List<WSelectElement> GetSelectElement()
        {
            var newSelectElementClause = new List<WSelectElement>();
            foreach (var projection in ProjectionList)
            {
                newSelectElementClause.Add(projection.ToSelectElement());
            }
            return newSelectElementClause;
        }

        public WWhereClause GetWhereClause()
        {
            return new WWhereClause() { SearchCondition = Predicates };
        }

        public WOrderByClause GetOrderByClause()
        {
            if (OrderByVariable == null) 
                return new WOrderByClause();
            OrderByRecord orderByRecord = OrderByVariable.Item2;
            WOrderByClause newOrderByClause = new WOrderByClause()
            {
                OrderByElements = orderByRecord.SortOrderList
            };
            return newOrderByClause;
        }

        public WGroupByClause GetGroupByClause()
        {
            if (GroupByVariable == null)
                return new WGroupByClause();
            GroupByRecord groupByRecord = GroupByVariable.Item2;
            WGroupByClause newGroupByClause = new WGroupByClause()
            {
                GroupingSpecifications = groupByRecord.GroupingSpecList
            };

            return newGroupByClause;
        }


        public WDeleteSpecification ToSqlDelete()
        {
            if (CurrVariable is GremlinVertexVariable)
            {
                return ToSqlDeleteNode();
            }
            else if (CurrVariable is GremlinEdgeVariable)
            {
                return ToSqlDeleteEdge();
            }
            else
            {
                return null;
            }
        }

        public WDeleteNodeSpecification ToSqlDeleteNode()
        {
            // delete node
            // where node.id in (subquery)
            //SetProjection("id");
            WSelectQueryExpression selectQueryExpr = ToSelectQueryBlock() as WSelectQueryBlock;
            WInPredicate inPredicate = new WInPredicate()
            {
                Subquery = new WScalarSubquery() { SubQueryExpr = selectQueryExpr },
                Expression = GremlinUtil.GetColumnReferenceExpression("node", "id")
            };

            WWhereClause newWhereClause = new WWhereClause() { SearchCondition = inPredicate };
            WNamedTableReference newTargetClause = GremlinUtil.GetNamedTableReference("node");

            return new WDeleteNodeSpecification()
            {
                WhereClause = newWhereClause,
                Target = newTargetClause
            };
        }

        public WDeleteEdgeSpecification ToSqlDeleteEdge()
        {
            return new WDeleteEdgeSpecification(ToSelectQueryBlock() as WSelectQueryBlock);
        }

        public void AddPaths(GremlinVariable source, GremlinVariable edge, GremlinVariable target)
        {
            if (source.GetType() == typeof(GremlinVertexVariable))
            {
                Paths.Add(new Tuple<GremlinVariable, GremlinVariable, GremlinVariable>
                    (source, edge, target));
            }
            else
            {
                throw new Exception("Edges can't have a out step.");
            }
        }

        public void AddPredicate(WBooleanExpression expr)
        {
            Predicates = Predicates == null ? expr : new WBooleanBinaryExpression()
            {
                BooleanExpressionType = BooleanBinaryExpressionType.And,
                FirstExpr = Predicates,
                SecondExpr = expr
            };
        }

        public void AddLabelsPredicatesToEdge(List<string> edgeLabels, GremlinEdgeVariable edgeVar)
        {
            foreach (var edgeLabel in edgeLabels)
            {
                WValueExpression predicateValue = new WValueExpression(edgeLabel, true);
                WBooleanComparisonExpression comExpression = new WBooleanComparisonExpression()
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = GremlinUtil.GetColumnReferenceExpression(edgeVar.VariableName, "label"),
                    SecondExpr = predicateValue
                };
                AddPredicate(comExpression);
            }

        }

        public void AddContextStatements(GremlinToSqlContext context)
        {
            foreach (var statement in context.Statements)
            {
                Statements.Add(statement);
            }
            Statements.Add(context.ToSelectQueryBlock());
        }

    }
}
