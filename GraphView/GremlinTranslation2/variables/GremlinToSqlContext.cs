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
        public GremlinVariable PivotVariable { get; set; }
        public Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>> TaggedVariables { get; set; }
        public List<GremlinVariable> VariableList { get; private set; }
        public List<ISqlTable> TableReferences { get; private set; }
        //public List<ISqlScalar> ProjectedVariables { get; private set; }
        public List<ISqlStatement> SetVariables { get; private set; }
        public List<GremlinMatchPath> Paths { get; set; }
        public GremlinGroupVariable GroupVariable { get; set; }
        public WBooleanExpression Predicates { get; private set; }

        public GremlinToSqlContext()
        {
            TaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>>();
            TableReferences = new List<ISqlTable>();
            SetVariables = new List<ISqlStatement>();
            //ProjectedVariables = new List<ISqlScalar>();
            VariableList = new List<GremlinVariable>();
            Paths = new List<GremlinMatchPath>();
        }

        public GremlinToSqlContext Duplicate()
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

        public void Reset()
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

        public void AddPredicate(WBooleanExpression newPredicate)
        {
            Predicates = Predicates == null ? newPredicate : SqlUtil.GetAndBooleanBinaryExpr(Predicates, newPredicate);
        }

        public GremlinVertexVariable GetSourceVertex(GremlinVariable edge)
        {
            return Paths.Find(path => path.EdgeVariable.VariableName == edge.VariableName)?.SourceVariable;
        }

        public GremlinVertexVariable GetSinkVertex(GremlinVariable edge)
        {
            return Paths.Find(path=> path.EdgeVariable.VariableName == edge.VariableName)?.SinkVariable;
        }
        public WBooleanExpression ToSqlBoolean()
        {
            return TableReferences.Count == 0 ? (WBooleanExpression) SqlUtil.GetBooleanParenthesisExpr(Predicates)
                                              : SqlUtil.GetExistPredicate(ToSelectQueryBlock());
        }

        //Generate SQL Script
        public WSqlScript ToSqlScript()
        {
            return new WSqlScript()
            {
                Batches = GetBatchList()
            };
        }

        public List<WSqlBatch> GetBatchList()
        {
            return new List<WSqlBatch>()
            {
                new WSqlBatch()
                {
                    Statements = GetStatements()
                }
            };
        }

        public List<WSqlStatement> GetSetVariableStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();
            foreach (var variable in SetVariables)
            {
                statementList.AddRange(variable.ToSetVariableStatements());
            }
            return statementList;
        }

        public List<WSqlStatement> GetStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();
            statementList.AddRange(GetSetVariableStatements());
            statementList.Add(ToSelectQueryBlock());
            return statementList;
        }

        public WSelectQueryBlock ToSelectQueryBlock(List<string> ProjectedProperties = null)
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

        public WFromClause GetFromClause()
        {
            if (TableReferences.Count == 0) return null;

            var newFromClause = new WFromClause();
            foreach (var tableReference in TableReferences)
            {
                newFromClause.TableReferences.Add(tableReference.ToTableReference());
            }
            return newFromClause;
        }

        public WMatchClause GetMatchClause()
        {
            if (Paths.Count == 0) return null;

            var newMatchClause = new WMatchClause();
            foreach (var path in Paths)
            {
                newMatchClause.Paths.Add(SqlUtil.GetMatchPath(path));
            }
            return newMatchClause;
        }

        public List<WSelectElement> GetSelectElement(List<string> ProjectedProperties)
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
                selectElements.Add(PivotVariable.DefaultProjection().ToSelectElement());
            }
            return selectElements;
        }

        public WWhereClause GetWhereClause()
        {
            return Predicates == null ? null : SqlUtil.GetWhereClause(Predicates);
        }

        public WOrderByClause GetOrderByClause()
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

        public WGroupByClause GetGroupByClause()
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
