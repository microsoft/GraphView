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
            AliasToGremlinVariableList = new List<Tuple<string, GremlinVariable>>();
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

        public List<Tuple<string, GremlinVariable>> AliasToGremlinVariableList;

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


        public void AddNewVariable(GremlinVariable gremlinVar, List<string> labels)
        {
            RemainingVariableList.Add(gremlinVar);
            if (labels.Count == 0) return;
            foreach (var label in labels)
            {
                AliasToGremlinVariableList.Add(new Tuple<string, GremlinVariable>(label, gremlinVar));
            }
        }

        public void SetCurrVariable(GremlinVariable gremlinVar)
        {
            CurrVariable = gremlinVar;
        }

        //Projection
        public void AddProjection(GremlinVariable gremlinVar, string value)
        {
            Projection.Add(new Tuple<GremlinVariable, Projection>(gremlinVar, new ColumnProjection(gremlinVar, value)));
        }
        public void AddProjection(GremlinVariable gremlinVar, Projection projection)
        {
            Projection.Add(new Tuple<GremlinVariable, Projection>(gremlinVar, projection));
        }
        public void SetDefaultProjection(GremlinVariable newGremlinVar)
        {
            Projection.Clear();
            AddProjection(newGremlinVar, new ColumnProjection(newGremlinVar, "id"));
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
                AddProjection(CurrVariable, new ColumnProjection(CurrVariable, value as string));
            }
        }

        public void SetConstantProjection(object value)
        {
            Projection.Clear();
            AddProjection(CurrVariable, new ConstantProjection(CurrVariable, value as string));
        }

        public void SetStarProjection(object value)
        {
            Projection.Clear();
            AddProjection(CurrVariable, new StarProjection());
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
            columnK.Add(GremlinUtil.GetColumnReferenceExpression("label"));
            var valueExpr = GremlinUtil.GetValueExpression(currVar.EdgeLabel);
            selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(valueExpr));
            foreach (var property in currVar.Properties)
            {
                foreach (var value in property.Value)
                {
                    columnK.Add(GremlinUtil.GetColumnReferenceExpression(property.Key));
                    valueExpr = GremlinUtil.GetValueExpression(value.ToString());
                    selectBlock.SelectElements.Add(GremlinUtil.GetSelectScalarExpression(valueExpr));
                }
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

            if (currVar.VertexLabel != null)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpression("label"));
                columnV.Add(GremlinUtil.GetValueExpression(currVar.VertexLabel));
            }

            foreach (var property in currVar.Properties)
            {
                foreach (var value in property.Value)
                {
                    columnK.Add(GremlinUtil.GetColumnReferenceExpression(property.Key));
                    columnV.Add(GremlinUtil.GetValueExpression(value));
                }
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
                if (currVar is GremlinJoinVertexVariable)
                {
                    //across apply tvf as v3
                    GremlinVariable lastVar = RemainingVariableList[i - 1];
                    newFromClause.TableReferences.RemoveAt(newFromClause.TableReferences.Count - 1);

                    WUnqualifiedJoin unqualifiedJoin = GremlinUtil.GetUnqualifiedJoin(currVar, lastVar);
                    newFromClause.TableReferences.Add(unqualifiedJoin);
                }
                else if (currVar is GremlinVertexVariable)
                {
                    WNamedTableReference tableReference = GremlinUtil.GetNamedTableReference(currVar);
                    newFromClause.TableReferences.Add(tableReference);
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
                    newFromClause.TableReferences.Add((currVar as GremlinChooseVariable).ChooseExpr);
                }
                else if (currVar is GremlinCoalesceVariable)
                {
                    newFromClause.TableReferences.Add((currVar as GremlinCoalesceVariable).CoalesceExpr);
                }
                else if (currVar is GremlinDerivedVariable)
                {
                    newFromClause.TableReferences.Add((currVar as GremlinDerivedVariable).QueryDerivedTable);
                }
                else if (currVar is GremlinOptionalVariable)
                {
                    newFromClause.TableReferences.Add((currVar as GremlinOptionalVariable).OptionalExpr);
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
                newSelectElementClause.Add(dict.Item2.ToSelectElement());
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
                    FirstExpr = GremlinUtil.GetColumnReferenceExpression(edgeVar.VariableName, "label"),
                    SecondExpr = predicateValue
                };
                AddPredicate(comExpression);
            }

        }

    }
}
