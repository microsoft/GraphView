using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinToSqlContext
    {
        internal GremlinVariable PivotVariable { get; set; }
        internal Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>> TaggedVariables { get; set; }
        internal List<GremlinVariable> VariableList { get; private set; }
        internal List<ISqlTable> TableReferences { get; private set; }
        //public List<ISqlScalar> ProjectedVariables { get; private set; }
        internal List<ISqlStatement> SetVariables { get; private set; }
        internal List<GremlinMatchPath> Paths { get; set; }
        internal GremlinGroupVariable GroupVariable { get; set; }
        internal WBooleanExpression Predicates { get; private set; }

        internal GremlinToSqlContext()
        {
            TaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>>();
            TableReferences = new List<ISqlTable>();
            SetVariables = new List<ISqlStatement>();
            //ProjectedVariables = new List<ISqlScalar>();
            VariableList = new List<GremlinVariable>();
            Paths = new List<GremlinMatchPath>();
        }

        internal GremlinToSqlContext Duplicate()
        {
            return new GremlinToSqlContext()
            {
                VariableList = new List<GremlinVariable>(this.VariableList),
                TaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>>(TaggedVariables),
                PivotVariable = this.PivotVariable,
                TableReferences = new List<ISqlTable>(this.TableReferences),
                //SetVariables = new List<ISqlStatement>(this.SetVariables), //Don't Duplicate for avoiding redundant set-sqlstatment
                GroupVariable = GroupVariable,   // more properties need to be added when GremlinToSqlContext is changed.
                Paths = new List<GremlinMatchPath>(this.Paths),
                Predicates = this.Predicates
            };
        }

        internal void Reset()
        {
            PivotVariable = null;
            GroupVariable = null;
            Predicates = null;
            TaggedVariables.Clear();
            VariableList.Clear();
            TableReferences.Clear();
            //SetVariables.Clear();
            //ProjectedVariables.Clear();
            Paths.Clear();
            // More resetting goes here when more properties are added to GremlinToSqlContext
        }

        internal void Populate(string propertyName)
        {
            // For a query with a GROUP BY clause, the ouptut format is determined
            // by the aggregation functions following GROUP BY and cannot be changed.
            if (GroupVariable != null)
            {
                return;
            }

            PivotVariable.Populate(propertyName);
        }

        internal GremlinVariable SelectVariable(string selectKey, GremlinKeyword.Pop pop = GremlinKeyword.Pop.Default)
        {
            if (!TaggedVariables.ContainsKey(selectKey))
            {
                throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", selectKey));
            }
            switch (pop)
            {
                case GremlinKeyword.Pop.first:
                    return TaggedVariables[selectKey].First().Item1;
                case GremlinKeyword.Pop.last:
                    return TaggedVariables[selectKey].Last().Item1;
                default:
                    return TaggedVariables[selectKey].Last().Item1;
            }
        }

        internal void AddPredicate(WBooleanExpression newPredicate)
        {
            Predicates = Predicates == null ? newPredicate : SqlUtil.GetAndBooleanBinaryExpr(Predicates, newPredicate);
        }

        internal void AddLabelPredicateForEdge(GremlinEdgeVariable edge, List<string> edgeLabels)
        {
            if (edgeLabels.Count == 0) return;
            edge.Populate("label");
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = SqlUtil.GetColumnReferenceExpr(edge.VariableName, "label");
                var secondExpr = SqlUtil.GetValueExpr(edgeLabel);
                booleanExprList.Add(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            AddPredicate(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));
        }

        internal GremlinVertexVariable GetSourceVertex(GremlinVariable edge)
        {
            return Paths.Find(path => path.EdgeVariable.VariableName == edge.VariableName)?.SourceVariable;
        }

        internal GremlinVertexVariable GetSinkVertex(GremlinVariable edge)
        {
            return Paths.Find(path=> path.EdgeVariable.VariableName == edge.VariableName)?.SinkVariable;
        }
        internal WBooleanExpression ToSqlBoolean()
        {
            return TableReferences.Count == 0 ? (WBooleanExpression) SqlUtil.GetBooleanParenthesisExpr(Predicates)
                                              : SqlUtil.GetExistPredicate(ToSelectQueryBlock());
        }

        internal WSqlScript ToSqlScript()
        {
            return new WSqlScript()
            {
                Batches = GetBatchList()
            };
        }

        internal List<WSqlBatch> GetBatchList()
        {
            return new List<WSqlBatch>()
            {
                new WSqlBatch()
                {
                    Statements = GetStatements()
                }
            };
        }

        internal List<WSqlStatement> GetSetVariableStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();
            foreach (var variable in SetVariables)
            {
                statementList.AddRange(variable.ToSetVariableStatements());
            }
            return statementList;
        }

        internal List<WSqlStatement> GetStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();
            statementList.AddRange(GetSetVariableStatements());
            statementList.Add(ToSelectQueryBlock());
            return statementList;
        }

        internal WSelectQueryBlock ToSelectQueryBlock(List<string> ProjectedProperties = null)
        {
            return new WSelectQueryBlock()
            {
                SelectElements = GetSelectElement(ProjectedProperties),
                FromClause = GetFromClause(),
                MatchClause = GetMatchClause(),
                WhereClause = GetWhereClause(),
                OrderByClause = GetOrderByClause(),
                GroupByClause = GetGroupByClause()
            };
        }

        internal WFromClause GetFromClause()
        {
            if (TableReferences.Count == 0) return null;

            var newFromClause = new WFromClause();
            foreach (var tableReference in TableReferences)
            {
                newFromClause.TableReferences.Add(tableReference.ToTableReference());
            }
            return newFromClause;
        }

        internal WMatchClause GetMatchClause()
        {
            if (Paths.Count == 0) return null;

            var newMatchClause = new WMatchClause();
            foreach (var path in Paths)
            {
                newMatchClause.Paths.Add(SqlUtil.GetMatchPath(path));
            }
            return newMatchClause;
        }

        internal List<WSelectElement> GetSelectElement(List<string> ProjectedProperties)
        {
            var selectElements = new List<WSelectElement>();
            if (ProjectedProperties != null && ProjectedProperties.Count != 0)
            {
                foreach (var projectProperty in ProjectedProperties)
                {
                    var valueExpr = SqlUtil.GetColumnReferenceExpr(PivotVariable.VariableName, projectProperty);
                    selectElements.Add(SqlUtil.GetSelectScalarExpr(valueExpr));
                }
            }
            else
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(PivotVariable.DefaultProjection().ToScalarExpression()));
            }
            return selectElements;
        }

        internal WWhereClause GetWhereClause()
        {
            return Predicates == null ? null : SqlUtil.GetWhereClause(Predicates);
        }

        internal WOrderByClause GetOrderByClause()
        {
            return null;
            //if (OrderByVariable == null) return null;

            //OrderByRecord orderByRecord = OrderByVariable.Item2;
            //WOrderByClause newOrderByClause = new WOrderByClause()
            //{
            //    OrderByElements = orderByRecord.SortOrderList
            //};
            //return newOrderByClause;
        }

        internal WGroupByClause GetGroupByClause()
        {
            return null;
            //if (GroupByVariable == null) return null;

            //GroupByRecord groupByRecord = GroupByVariable.Item2;
            //WGroupByClause newGroupByClause = new WGroupByClause()
            //{
            //    GroupingSpecifications = groupByRecord.GroupingSpecList
            //};

            //return newGroupByClause;
        }
    }
}
