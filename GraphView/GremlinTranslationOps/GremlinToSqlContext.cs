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
            Projection = new List<Tuple<GremlinVariable, Projection>>();
            Paths = new List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>>();
            AliasToGremlinVariable = new Dictionary<string, GremlinVariable>();
            //GroupByVariable = new Tuple<GremlinVariable, GroupByRecord>();
            //OrderByVariable = new Tuple<GremlinVariable, OrderByRecord>();
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

        public Dictionary<string, GremlinVariable> AliasToGremlinVariable;

        /// <summary>
        /// A list of Gremlin variables and their properties the query projects. 
        /// When no property is specified, the variable projects its "ID":
        /// If the variable is a vertex variable, it projects the vertex ID;
        /// If the variable is an edge variable, it projects the (source vertex ID, sink vertex ID, offset) pair. 
        /// 
        /// The projection is updated, as it is passed through every traversal. 
        /// </summary> 
        public List<Tuple<GremlinVariable, Projection>> Projection { get; set; }

        public List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>> Paths { get; set; }
        /// <summary>
        /// The Gremlin variable and its property by which the query groups
        /// </summary>
        public Tuple<GremlinVariable, GroupByRecord> GroupByVariable { get; set; }

        /// <summary>
        /// The Gremlin variable and its property by which the query orders
        /// </summary>
        public Tuple<GremlinVariable, OrderByRecord> OrderByVariable { get; set; }


        public void AddNewVariable(GremlinVariable gremlinVar)
        {
            RemainingVariableList.Add(gremlinVar);
        }

        public void SetCurrVariable(GremlinVariable gremlinVar)
        {
            CurrVariable = gremlinVar;
        }

        //Projection
        public void AddProjection(GremlinVariable gremlinVar, string value)
        {
            Projection.Add(new Tuple<GremlinVariable, Projection>(gremlinVar, new ValueProjection(gremlinVar, value)));
        }
        public void AddProjection(GremlinVariable gremlinVar, Projection projection)
        {
            Projection.Add(new Tuple<GremlinVariable, Projection>(gremlinVar, projection));
        }
        public void SetDefaultProjection(GremlinVariable newGremlinVar)
        {
            Projection.Clear();
            AddProjection(newGremlinVar, new ValueProjection(newGremlinVar, "id"));
        }

        public void SetCurrProjection(object value)
        {
            Projection.Clear();
            if (value.GetType() == typeof(WFunctionCall))
            {
                AddProjection(CurrVariable, new FunctionCallProjection(CurrVariable, value as WFunctionCall));
            }
            else
            {
                AddProjection(CurrVariable, new ValueProjection(CurrVariable, value as string));
            }
        }

        public void SetConstantProjection(object value)
        {
            Projection.Clear();
            AddProjection(CurrVariable, new ConstantProjection(CurrVariable, value as string));
        }
        public void ClearProjection()
        {
            Projection.Clear();
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
                WSqlStatement subQueryExpr = ToSelectSqlQuery();
                return GremlinUtil.GetExistPredicate(subQueryExpr);
            }
        }

        public WScalarExpression ToSqlScalar()
        {
            return null;
        }

        public WSqlStatement ToSqlQuery()
        {
            if (CurrVariable is GremlinAddEVariable)
            {
                return ToAddESqlQuery();
            }
            else if (CurrVariable is GremlinAddVVariable)
            {
                return ToAddVSqlQuery();
            }
            else
            {
                return ToSelectSqlQuery();
            }
        }

        public WSqlStatement ToSelectSqlQuery()
        {
            //Consturct the new From Cluase;
            var newFromClause = GetFromClause();

            // Construct the new Match Cluase
            var newMatchClause = GetMatchClause();

            // Construct the new Select Component
            var newSelectElementClause = GetSelectElement();

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

        public WSqlStatement ToAddESqlQuery()
        {
            var columnK = new List<WColumnReferenceExpression>();
            var currVar = CurrVariable as GremlinAddEVariable;
            var selectBlock = ToSelectSqlQuery() as WSelectQueryBlock;
            selectBlock.SelectElements.Clear();

            var fromVarExpr = GremlinUtil.GetColumnReferenceExpression(currVar.FromVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(fromVarExpr));

            var toVarExpr = GremlinUtil.GetColumnReferenceExpression(currVar.ToVariable.VariableName);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(toVarExpr));

            //Add edge key-value
            columnK.Add(GremlinUtil.GetColumnReferenceExpression("type"));
            var valueExpr = GremlinUtil.GetValueExpression(currVar.EdgeLabel);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(valueExpr));
            foreach (var property in currVar.Properties)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression(property.Key));
                valueExpr = GremlinUtil.GetValueExpression(property.Value);
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

        public WSqlStatement ToAddVSqlQuery()
        {
            var columnK = new List<WColumnReferenceExpression>();
            var columnV = new List<WScalarExpression>();
            var currVar = CurrVariable as GremlinAddVVariable;

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
                if (RemainingVariableList[i] is GremlinVertexVariable)
                {
                    if (RemainingVariableList[i] is GremlinJoinVertexVariable)
                    {
                        //across apply tvf as v3
                        GremlinJoinVertexVariable currVar = RemainingVariableList[i] as GremlinJoinVertexVariable;;
                        GremlinVariable lastVar = RemainingVariableList[i - 1];
                        newFromClause.TableReferences.RemoveAt(newFromClause.TableReferences.Count - 1);

                        WSchemaObjectFunctionTableReference secondTableRef = new WSchemaObjectFunctionTableReference()
                        {
                            Alias = GremlinUtil.GetIdentifier(RemainingVariableList[i].VariableName),
                            Parameters = new List<WScalarExpression>()
                            {
                                GremlinUtil.GetColumnReferenceExpression(currVar.LeftVariable.VariableName),
                                GremlinUtil.GetColumnReferenceExpression(currVar.RightVariable.VariableName )
                            },
                            SchemaObject = new WSchemaObjectName()
                            {
                                Identifiers = new List<Identifier>() { GremlinUtil.GetIdentifier("BothV") }
                            }
                        };

                        WUnqualifiedJoin uniUnqualifiedJoin = new WUnqualifiedJoin()
                        {
                            FirstTableRef = GremlinUtil.GetNamedTableReference(lastVar),
                            SecondTableRef = secondTableRef,
                            UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
                        };
                        newFromClause.TableReferences.Add(uniUnqualifiedJoin);
                    }
                    else
                    {
                        WNamedTableReference TR = null;
                        TR = GremlinUtil.GetNamedTableReference(RemainingVariableList[i]);
                        newFromClause.TableReferences.Add(TR);
                    }
                }
            }
            return newFromClause;
        }

        public WMatchClause GetMatchClause()
        {
            var newMatchClause = new WMatchClause() { Paths = new List<WMatchPath>() };
            foreach (var path in Paths)
            {
                var pathEdges = new List<Tuple<WSchemaObjectName, WEdgeColumnReferenceExpression>>();
                pathEdges.Add(GremlinUtil.GetPathExpression(path));
                var tailNode = GremlinUtil.GetSchemaObjectName(path.Item3.VariableName);
                var newPath = new WMatchPath() { PathEdgeList = pathEdges, Tail = tailNode };
                newMatchClause.Paths.Add((newPath));
            }

            return newMatchClause;
        }

        public List<WSelectElement> GetSelectElement()
        {
            var newSelectElementClause = new List<WSelectElement>();
            foreach (var dict in Projection)
            {
                newSelectElementClause.Add(dict.Item2.ToSelectScalarExpression());
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
            WSelectQueryExpression selectQueryExpr = ToSqlQuery() as WSelectQueryBlock;
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
            return new WDeleteEdgeSpecification(ToSqlQuery() as WSelectQueryBlock);
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
                    FirstExpr = GremlinUtil.GetColumnReferenceExpression(edgeVar.VariableName, "type"),
                    SecondExpr = predicateValue
                };
                AddPredicate(comExpression);
            }

        }

    }
}
