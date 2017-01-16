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
        internal List<GremlinToSqlContext> PathContexts { get; set; }
        internal GremlinToSqlContext ParentContext { get; set; }
        internal GremlinVariable ParentVariable { get; set; }
        internal GremlinVariable PivotVariable { get; set; }
        internal Dictionary<string, List<GremlinVariable>> TaggedVariables { get; set; }
        internal Dictionary<string, List<GremlinVariable>> InheritedTaggedVariables { get; set; }
        internal List<GremlinVariable> InheritedVariableList { get; set; }
        internal List<GremlinVariable> VariableList { get; private set; }
        internal List<GremlinVariable> StepList { get; set; }
        internal List<GremlinMatchPath> PathList { get; set; }
        internal List<GremlinMatchPath> MatchList { get; set; }
        internal List<GremlinTableVariable> TableReferences { get; private set; }
        internal GremlinGroupVariable GroupVariable { get; set; }
        internal WBooleanExpression Predicates { get; private set; }
        internal GremlinPathVariable CurrentContextPath { get; set; }
        internal List<Tuple<GremlinVariableProperty, string>> ProjectVariablePropertiesList { get; set; }

        private bool isPopulateGremlinPath;

        internal GremlinToSqlContext()
        {
            TaggedVariables = new Dictionary<string, List<GremlinVariable>>();
            InheritedTaggedVariables = new Dictionary<string, List<GremlinVariable>>();
            TableReferences = new List<GremlinTableVariable>();
            VariableList = new List<GremlinVariable>();
            InheritedVariableList = new List<GremlinVariable>();
            PathList = new List<GremlinMatchPath>();
            MatchList = new List<GremlinMatchPath>();
            StepList = new List<GremlinVariable>();
            isPopulateGremlinPath = false;
            ProjectVariablePropertiesList = new List<Tuple<GremlinVariableProperty, string>>();
        }

        internal GremlinToSqlContext Duplicate()
        {
            return new GremlinToSqlContext()
            {
                VariableList = new List<GremlinVariable>(this.VariableList),
                InheritedVariableList = new List<GremlinVariable>(this.InheritedVariableList),
                TaggedVariables = new Dictionary<string, List<GremlinVariable>>(TaggedVariables),
                InheritedTaggedVariables = new Dictionary<string, List<GremlinVariable>>(InheritedTaggedVariables),
                PivotVariable = this.PivotVariable,
                TableReferences = new List<GremlinTableVariable>(this.TableReferences),
                GroupVariable = GroupVariable,
                PathList = new List<GremlinMatchPath>(this.PathList),
                MatchList = new List<GremlinMatchPath>(this.MatchList),
                Predicates = this.Predicates,
                StepList = new List<GremlinVariable>(this.StepList),
                isPopulateGremlinPath = this.isPopulateGremlinPath,
                CurrentContextPath = this.CurrentContextPath,
                ProjectVariablePropertiesList = new List<Tuple<GremlinVariableProperty, string>>(this.ProjectVariablePropertiesList)
            };
        }

        internal void Reset()
        {
            PivotVariable = null;
            GroupVariable = null;
            Predicates = null;
            TaggedVariables.Clear();
            InheritedTaggedVariables.Clear();
            VariableList.Clear();
            InheritedVariableList.Clear();
            TableReferences.Clear();
            PathList.Clear();
            MatchList.Clear();
            StepList.Clear();
            isPopulateGremlinPath = false;
            CurrentContextPath = null;
            ProjectVariablePropertiesList.Clear();
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

        internal void BottomUpPopulate(string propertyName)
        {
            
        }

        internal List<GremlinVariable> SelectCurrentAndChildVariable(string label)
        {
            List<GremlinVariable> taggedVariableList = new List<GremlinVariable>();
            for (var i = 0; i < VariableList.Count; i++)
            {
                if (VariableList[i].Labels.Contains(label))
                {
                    taggedVariableList.Add(VariableList[i]);
                }
                else
                {
                    if (VariableList[i].ContainsLabel(label))
                    {
                        List<GremlinVariable> subContextVariableList = VariableList[i].PopulateAllTaggedVariable(label);
                        taggedVariableList.AddRange(subContextVariableList);
                    }
                }
            }
            return taggedVariableList;
        }

        //internal GremlinVariable PopulateCurrAndChildContextAllTaggedVariable(string label)
        //{
        //    List<GremlinVariable> taggedVariableList = new List<GremlinVariable>();
        //    //Populate Variable and add them to ProjectVariableList
        //    foreach (var currentContextVariable in VariableList)
        //    {
        //        if (currentContextVariable.ContainsLabel(label))
        //        {
        //            var taggedVariable = currentContextVariable.PopulateAllTaggedVariable(label);
        //            taggedVariableList.Add(taggedVariable);
        //            ProjectVariableList.Add(new Tuple<string, GremlinVariable>(label, taggedVariable));
        //        }
        //    }
        //    if (taggedVariableList.Count == 0) return null;
        //    if (taggedVariableList.Count == 1) return taggedVariableList.First();
        //    return new GremlinListVariable(taggedVariableList);
        //}

        //internal void PopulateCurrAndChildContextFirstTaggedVariable(string label)
        //{
        //Populate Variable and add them to ProjectVariableList
        //foreach (var currentContextVariable in VariableList)
        //{
        //    if (currentContextVariable.ContainsLabel(label))
        //    {
        //        ProjectVariableList.Add(currentContextVariable.PopulateFirstTaggedVariable(label));
        //    }
        //}
        //}

        //internal void PopulateCurrAndChildContextLastTaggedVariable(string label)
        //{
        //    //Populate Variable and add them to ProjectVariableList
        //    //foreach (var currentContextVariable in VariableList)
        //    //{
        //    //    if (currentContextVariable.ContainsLabel(label))
        //    //    {
        //    //        ProjectVariableList.Add(currentContextVariable.PopulateLastTaggedVariable(label));
        //    //    }
        //    //}
        //}

        internal List<GremlinVariable> SelectParent(string label)
        {
            List<GremlinVariable> taggedVariableList = ParentContext?.SelectParent(label);
            if (taggedVariableList == null) taggedVariableList = new List<GremlinVariable>();

            //Count - 1 to ignore the last one, for ignoring the other subContext
            for (var i = 0; i < VariableList.Count - 1; i++)
            {
                if (VariableList[i].Labels.Contains(label))
                {
                    taggedVariableList.Add(GremlinContextVariable.Create(VariableList[i]));
                }
                else
                {
                    if (VariableList[i].ContainsLabel(label))
                    {
                        List<GremlinVariable> subContextVariableList = VariableList[i].PopulateAllTaggedVariable(label);
                        foreach (var subContextVar in subContextVariableList)
                        {
                            GremlinGhostVariable newVariable = GremlinGhostVariable.Create(subContextVar, VariableList[i], label);
                            taggedVariableList.Add(newVariable);
                        }
                    }
                }
            }
            return taggedVariableList;
        }

        internal List<GremlinVariable> Select(string label)
        {
            List<GremlinVariable> taggedVariableList = ParentContext?.SelectParent(label);
            if (taggedVariableList == null) taggedVariableList = new List<GremlinVariable>();

            for (var i = 0; i < VariableList.Count; i++)
            {
                if (VariableList[i].Labels.Contains(label))
                {
                    //in the current context
                    taggedVariableList.Add(VariableList[i]);
                }
                else
                {
                    //in the subContext of current Context
                    if (VariableList[i].ContainsLabel(label))
                    {
                        List<GremlinVariable> subContextVariableList = VariableList[i].PopulateAllTaggedVariable(label);
                        foreach (var subContextVar in subContextVariableList)
                        {
                            GremlinGhostVariable newVariable = GremlinGhostVariable.Create(subContextVar, VariableList[i], label);
                            taggedVariableList.Add(newVariable);
                        }
                    }
                }
            }
            return taggedVariableList;
        }


        //internal GremlinVariable Select(string label)
        //{
        //    GremlinVariable parentVariable = ParentContext?.Select(label);
        //    List<GremlinVariable> taggedVariableList = new List<GremlinVariable>();
        //    if (parentVariable is GremlinWrapVariable)
        //    {
        //        taggedVariableList.Add(parentVariable);
        //    }
        //    else if (parentVariable is GremlinListVariable)
        //    {
        //        taggedVariableList.AddRange((parentVariable as GremlinListVariable).GremlinVariableList);
        //    }
        //    else if (parentVariable != null)
        //    {
        //        taggedVariableList.Add(parentVariable);
        //    }
        //    foreach (var currentContextVariable in VariableList)
        //    {
        //        if (currentContextVariable.ContainsLabel(label))
        //        {
        //            var taggedVariable = currentContextVariable.PopulateAllTaggedVariable(label);
        //            if (taggedVariable is GremlinListVariable)
        //            {
        //                taggedVariableList.AddRange((taggedVariable as GremlinListVariable).GremlinVariableList);
        //            }
        //            else
        //            {
        //                taggedVariableList.Add(taggedVariable);
        //            }
        //        }
        //    }
        //    if (taggedVariableList.Count == 0) return null;
        //    if (taggedVariableList.Count == 1) return taggedVariableList.First();
        //    return new GremlinListVariable(taggedVariableList);
        //}

        //internal GremlinVariable SelectLastTaggedVariable(string label)
        //{
        //    GremlinVariable selectVariable = null;
        //    for (var i = VariableList.Count; i >= 0; i--)
        //    {
        //        selectVariable = VariableList[i].PopulateLastTaggedVariable(label);
        //        if (selectVariable != null) return selectVariable;
        //    }
        //    return ParentContext?.SelectLastTaggedVariable(label);
        //}

        //internal GremlinVariable SelectFirstTaggedVariable(string label)
        //{
        //    var selectVariable = ParentContext?.SelectFirstTaggedVariable(label);
        //    if (selectVariable != null) return selectVariable;
        //    for (var i = 0; i < VariableList.Count; i++)
        //    {
        //        selectVariable = VariableList[i].PopulateFirstTaggedVariable(label);
        //        if (selectVariable != null) return selectVariable;
        //    }
        //    return null;
        //}

        internal void PopulateGremlinPath()
        {
            if (isPopulateGremlinPath) return;

            GremlinPathVariable newVariable = new GremlinPathVariable(GetGremlinStepList());
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            CurrentContextPath = newVariable;

            isPopulateGremlinPath = true;
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
                    return TaggedVariables[selectKey].First();
                case GremlinKeyword.Pop.last:
                    return TaggedVariables[selectKey].Last();
                default:
                    return TaggedVariables[selectKey].Last();
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
            newPivotVariable.ParentContext = this;
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
            foreach (var item in ProjectVariablePropertiesList)
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(item.Item1.ToScalarExpression(), item.Item2));
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
