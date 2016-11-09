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
            // GroupByVariable = new Tuple<GremlinVariable, string>();
            // OrderByVariable = new Tuple<GremlinVariable, string>();
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
        public Tuple<GremlinVariable, string> GroupByVariable { get; set; }

        /// <summary>
        /// The Gremlin variable and its property by which the query orders
        /// </summary>
        public Tuple<GremlinVariable, string> OrderByVariable { get; set; }

        public void AddNewVariable(GremlinVariable gremlinVar)
        {
            RemainingVariableList.Add(gremlinVar);
        }

        public void SetCurrentVariable(GremlinVariable gremlinVar)
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
        public void ClearProjection()
        {
            Projection.Clear();
        }

        //------

        public WBooleanExpression ToSqlBoolean()
        {
            WSelectQueryExpression subQueryExpr = ToSqlQuery();
            return GremlinUtil.GetExistPredicate(subQueryExpr);
        }

        public WScalarExpression ToSqlScalar()
        {
            return null;
        }

        public WSelectQueryBlock ToSqlQuery()
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

            // Construct the SelectBlock
            return new WSelectQueryBlock()
            {
                FromClause = newFromClause,
                SelectElements = newSelectElementClause,
                WhereClause = newWhereClause,
                MatchClause = newMatchClause,
                OrderByClause = newOrderByClause,
            };
        }
        public WFromClause GetFromClause()
        {
            var newFromClause = new WFromClause() { TableReferences = new List<WTableReference>() };
            foreach (var variable in RemainingVariableList)
            {
                WNamedTableReference TR = null;
                if (variable is GremlinVertexVariable)
                {
                    TR = new WNamedTableReference()
                    {
                        Alias = new Identifier() { Value = variable.VariableName },
                        TableObjectString = "node",
                        TableObjectName = new WSchemaObjectName(new Identifier() { Value = "node" })
                    };
                }
                newFromClause.TableReferences.Add(TR);
            }
            return newFromClause;
        }

        public WMatchClause GetMatchClause()
        {
            var newMatchClause = new WMatchClause() { Paths = new List<WMatchPath>() };
            return newMatchClause;
        }

        public List<WSelectElement> GetSelectElement()
        {
            var newSelectElementClause = new List<WSelectElement>();
            foreach (var dict in Projection)
            {
                WScalarExpression projection = dict.Item2.ToSelectScalarExpression();
                newSelectElementClause.Add(new WSelectScalarExpression() { SelectExpr = projection });
            }
            return newSelectElementClause;
        }

        public WWhereClause GetWhereClause()
        {
            return new WWhereClause() { SearchCondition = Predicates };
        }

        public WOrderByClause GetOrderByClause()
        {
            var newOrderByClause = new WOrderByClause();
            return newOrderByClause;
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
            WSelectQueryExpression selectQueryExpr = ToSqlQuery();
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
            return new WDeleteEdgeSpecification(ToSqlQuery());
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
