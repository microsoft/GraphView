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
        internal List<GremlinVariable> StepList { get; set; }
        internal List<GremlinMatchPath> PathList { get; set; }
        internal List<GremlinMatchPath> MatchList { get; set; }
        internal List<GremlinTableVariable> TableReferences { get; private set; }
        internal GremlinGroupVariable GroupVariable { get; set; }
        internal WBooleanExpression Predicates { get; private set; }
        internal GremlinPathVariable CurrentContextPath { get; set; }

        private bool isPopulateGremlinPath;

        internal GremlinToSqlContext()
        {
            TaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>>();
            TableReferences = new List<GremlinTableVariable>();
            VariableList = new List<GremlinVariable>();
            PathList = new List<GremlinMatchPath>();
            MatchList = new List<GremlinMatchPath>();
            StepList = new List<GremlinVariable>();

            isPopulateGremlinPath = false;
        }

        internal GremlinToSqlContext Duplicate()
        {
            return new GremlinToSqlContext()
            {
                VariableList = new List<GremlinVariable>(this.VariableList),
                TaggedVariables = new Dictionary<string, List<Tuple<GremlinVariable, GremlinToSqlContext>>>(TaggedVariables),
                PivotVariable = this.PivotVariable,
                TableReferences = new List<GremlinTableVariable>(this.TableReferences),
                GroupVariable = GroupVariable,   // more properties need to be added when GremlinToSqlContext is changed.
                PathList = new List<GremlinMatchPath>(this.PathList),
                MatchList = new List<GremlinMatchPath>(this.MatchList),
                Predicates = this.Predicates,
                StepList = new List<GremlinVariable>(this.StepList),
                isPopulateGremlinPath = this.isPopulateGremlinPath,
                CurrentContextPath = this.CurrentContextPath
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
            PathList.Clear();
            MatchList.Clear();
            StepList.Clear();
            // More resetting goes here when more properties are added to GremlinToSqlContext
            isPopulateGremlinPath = false;
            CurrentContextPath = null;
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

        internal void SetPivotVariable(GremlinVariable newPivotVariable)
        {
            PivotVariable = newPivotVariable;
            if (PivotVariable is GremlinContextVariable)
            {
                //Ignore the inherited variable
                if (!(PivotVariable as GremlinContextVariable).IsFromSelect) return;
            }
            StepList.Add(newPivotVariable);
        }

        internal List<GremlinVariableProperty> GetGremlinStepList()
        {
            List<GremlinVariableProperty> gremlinStepList = new List<GremlinVariableProperty>();
            foreach (var step in StepList)
            {
                step.PopulateGremlinPath();
                gremlinStepList.Add(step.GetPath());
            }
            return gremlinStepList;
        }

        internal void PopulateGremlinPath()
        {
            if (isPopulateGremlinPath) return;

            GremlinPathVariable newVariable = new GremlinPathVariable(GetGremlinStepList());
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            CurrentContextPath = newVariable;

            isPopulateGremlinPath = true;
        }

        internal void AddPath(GremlinMatchPath path)
        {
            PathList.Add(path);
            MatchList.Add(path);
        }

        internal bool IsVariableInCurrentContext(GremlinTableVariable variable)
        {
            return TableReferences.Contains(variable);
        }

        internal GremlinMatchPath GetPathFromPathList(GremlinTableVariable edge)
        {
            return PathList.Find(p => p.EdgeVariable.VariableName == edge.VariableName);
        }

        internal void AddPredicate(WBooleanExpression newPredicate)
        {
            Predicates = Predicates == null ? newPredicate : SqlUtil.GetAndBooleanBinaryExpr(Predicates, newPredicate);
        }

        internal void AddLabelPredicateForEdge(GremlinEdgeTableVariable edge, List<string> edgeLabels)
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

        internal GremlinTableVariable GetSourceVertex(GremlinVariable edge)
        {
            return PathList.Find(path => path.EdgeVariable.VariableName == edge.VariableName)?.SourceVariable;
        }

        internal GremlinTableVariable GetSinkVertex(GremlinVariable edge)
        {
            return PathList.Find(path => path.EdgeVariable.VariableName == edge.VariableName)?.SinkVariable;
        }

        internal GremlinVariableProperty GetSourceVariableProperty(GremlinVariable edge)
        {
            if ((edge as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                return new GremlinVariableProperty(edge, "_source");
            }
            else
            {
                var sourceVariable = GetSourceVertex(edge);
                if (sourceVariable == null)
                {
                    return new GremlinVariableProperty(edge, "_sink");
                }
                else
                {
                    return sourceVariable.DefaultProjection();
                }
            }
        }

        internal GremlinVariableProperty GetEdgeVariableProperty(GremlinVariable edge)
        {
            if ((edge as GremlinEdgeTableVariable).EdgeType == WEdgeType.InEdge)
            {
                return new GremlinVariableProperty(edge, GremlinKeyword.ReverseEdgeAdj);
            }
            else
            {
                return new GremlinVariableProperty(edge, GremlinKeyword.EdgeID);

            }
        }


        internal WBooleanExpression ToSqlBoolean()
        {
            return TableReferences.Count == 0 ? (WBooleanExpression) SqlUtil.GetBooleanParenthesisExpr(Predicates)
                                              : SqlUtil.GetExistPredicate(ToSelectQueryBlock());
        }

        internal WSqlScript ToSqlScript()
        {
            return new WSqlScript() { Batches = GetBatchList() };
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

        internal List<WSqlStatement> GetStatements()
        {
            return new List<WSqlStatement>() { ToSelectQueryBlock() };
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
            var newMatchClause = new WMatchClause();
            foreach (var path in MatchList)
            {
                if (path.EdgeVariable is GremlinFreeEdgeVariable)
                {
                    if (!(path.SinkVariable is GremlinFreeVertexVariable))
                    {
                        path.SinkVariable = null;
                    }
                    if (!(path.SourceVariable is GremlinFreeVertexVariable))
                    {
                        path.SourceVariable = null;
                    }
                    newMatchClause.Paths.Add(SqlUtil.GetMatchPath(path));
                }
            }
            return newMatchClause.Paths.Count == 0 ? null : newMatchClause;
        }

        internal List<WSelectElement> GetSelectElement(List<string> ProjectedProperties)
        {
            var selectElements = new List<WSelectElement>();
            if (ProjectedProperties != null && ProjectedProperties.Count != 0)
            {
                foreach (var projectProperty in ProjectedProperties)
                {
                    var columnRefExpr = SqlUtil.GetColumnReferenceExpr(PivotVariable.VariableName, projectProperty);
                    selectElements.Add(SqlUtil.GetSelectScalarExpr(columnRefExpr));
                }
            }
            else
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(PivotVariable.DefaultProjection().ToScalarExpression()));
            }
            if (isPopulateGremlinPath)
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(CurrentContextPath.DefaultProjection().ToScalarExpression(), GremlinKeyword.Path));
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
