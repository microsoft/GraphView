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
        public GremlinVariable2 PivotVariable { get; set; }
        public Dictionary<string, List<Tuple<GremlinVariable2, GremlinToSqlContext>>> TaggedVariables { get; set; }
        public List<GremlinVariable2> VariableList { get; private set; }
        public List<ISqlTable> TableReferences { get; private set; }
        //public List<ISqlScalar> ProjectedVariables { get; private set; }
        public List<ISqlStatement> SetVariables { get; private set; }
        public List<GremlinMatchPath> Paths { get; set; }
        public GremlinGroupVariable GroupVariable { get; set; }
        public WBooleanExpression Predicates { get; private set; }

        public GremlinToSqlContext()
        {
            TaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable2, GremlinToSqlContext>>>();
            TableReferences = new List<ISqlTable>();
            SetVariables = new List<ISqlStatement>();
            //ProjectedVariables = new List<ISqlScalar>();
            VariableList = new List<GremlinVariable2>();
            Paths = new List<GremlinMatchPath>();
        }

        public GremlinToSqlContext Duplicate()
        {
            return new GremlinToSqlContext()
            {
                VariableList = new List<GremlinVariable2>(this.VariableList),
                TaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable2, GremlinToSqlContext>>>(TaggedVariables),
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

        internal GremlinVariable2 SelectVariable(string selectKey, GremlinKeyword.Pop pop = GremlinKeyword.Pop.Default)
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
            Predicates = Predicates == null ? newPredicate : GremlinUtil.GetAndBooleanBinaryExpr(Predicates, newPredicate);
        }

        public void AddEqualPredicate(WScalarExpression firstExpr, WScalarExpression secondExpr)
        {
            AddPredicate(GremlinUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        public GremlinVertexVariable2 GetSourceVertex(GremlinVariable2 edge)
        {
            foreach (var path in Paths)
            {
                if (path.EdgeVariable.VariableName == edge.VariableName) return path.SourceVariable;
            }
            return null;
        }

        public GremlinVertexVariable2 GetSinkVertex(GremlinVariable2 edge)
        {
            foreach (var path in Paths)
            {
                if (path.EdgeVariable.VariableName == edge.VariableName) return path.SinkVariable;
            }
            return null;
        }
        public WBooleanExpression ToSqlBoolean()
        {
            if (TableReferences.Count == 0)
            {
                return GremlinUtil.GetBooleanParenthesisExpr(Predicates);
            }
            else
            {
                return GremlinUtil.GetExistPredicate(ToSelectQueryBlock());
            }
        }

        //Generate SQL Script
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
            // Construct the new Select Component
            var newSelectElementClause = GetSelectElement(ProjectedProperties);

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

        public WFromClause GetFromClause()
        {
            if (TableReferences.Count == 0) return null;

            var newFromClause = new WFromClause() { TableReferences = new List<WTableReference>() };
            foreach (var tableReference in TableReferences)
            {
                WTableReference tr = tableReference.ToTableReference();
                if (tr == null)
                {
                    throw new NotImplementedException();
                }
                newFromClause.TableReferences.Add(tableReference.ToTableReference());
            }
            return newFromClause;
        }

        public WMatchClause GetMatchClause()
        {
            if (Paths.Count == 0) return null;

            var newMatchClause = new WMatchClause() { Paths = new List<WMatchPath>() };
            foreach (var path in Paths)
            {
                newMatchClause.Paths.Add(GremlinUtil.GetMatchPath(path));
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
                    var valueExpr = GremlinUtil.GetColumnReferenceExpr(PivotVariable.VariableName, projectProperty);
                    selectElements.Add(GremlinUtil.GetSelectScalarExpr(valueExpr));
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
            if (Predicates == null) return null;
            return new WWhereClause() { SearchCondition = Predicates };
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
