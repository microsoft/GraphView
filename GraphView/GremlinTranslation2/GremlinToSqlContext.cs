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
        internal GremlinToSqlContext ParentContext { get; set; }
        internal GremlinVariable HomeVariable { get; set; }
        internal GremlinVariable PivotVariable { get; set; }
        internal List<GremlinVariable> VariableList { get; private set; }
        internal List<GremlinVariable> StepList { get; set; }
        internal List<GremlinMatchPath> PathList { get; set; }
        internal List<GremlinMatchPath> MatchList { get; set; }
        internal List<GremlinTableVariable> TableReferences { get; private set; }
        internal WBooleanExpression Predicates { get; private set; }
        internal GremlinPathVariable CurrentContextPath { get; set; }
        internal List<Tuple<GremlinVariableProperty, string>> ProjectVariablePropertiesList { get; set; }

        private bool isPopulateGremlinPath;

        internal GremlinToSqlContext()
        {
            TableReferences = new List<GremlinTableVariable>();
            VariableList = new List<GremlinVariable>();
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
                PivotVariable = this.PivotVariable,
                TableReferences = new List<GremlinTableVariable>(this.TableReferences),
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
            Predicates = null;
            VariableList.Clear();
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
            //// For a query with a GROUP BY clause, the ouptut format is determined
            //// by the aggregation functions following GROUP BY and cannot be changed.
            //if (GroupVariable != null)
            //{
            //    return;
            //}

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

        internal List<GremlinVariable> FetchAllVariablesInCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            for (var i = 0; i < VariableList.Count; i++)
            {
                variableList.Add(VariableList[i]);
                List<GremlinVariable> subContextVariableList = VariableList[i].FetchAllVariablesInCurrAndChildContext();
                if (subContextVariableList != null)
                {
                    variableList.AddRange(subContextVariableList);
                }
            }
            return variableList;
        }

        internal void AddProjectVariablePropertiesList(GremlinVariableProperty variableProperty, string alias)
        {
            foreach (var projectProperty in ProjectVariablePropertiesList)
            {
                if (projectProperty.Item1.VariableName == variableProperty.VariableName &&
                    projectProperty.Item1.VariableProperty == variableProperty.VariableProperty &&
                    projectProperty.Item2 == alias) return;
            }
            ProjectVariablePropertiesList.Add(new Tuple<GremlinVariableProperty, string>(variableProperty, alias));
        }

        internal List<GremlinVariable> SelectParent(string label, GremlinVariable stopVariable)
        {
            List<GremlinVariable> taggedVariableList = ParentContext?.SelectParent(label, HomeVariable);
            if (taggedVariableList == null) taggedVariableList = new List<GremlinVariable>();

            var stopIndex = stopVariable == null ? VariableList.Count : VariableList.IndexOf(stopVariable);

            for (var i = 0; i < stopIndex; i++)
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
                            if (subContextVar is GremlinGhostVariable)
                            {
                                var ghostVar = subContextVar as GremlinGhostVariable;
                                var newGhostVar = GremlinGhostVariable.Create(ghostVar.RealVariable,
                                    ghostVar.AttachedVariable, label);
                                taggedVariableList.Add(newGhostVar);
                            }
                            else
                            {
                                GremlinGhostVariable newVariable = GremlinGhostVariable.Create(subContextVar, VariableList[i], label);
                                taggedVariableList.Add(newVariable);
                            }
                        }
                    }
                }
            }
            return taggedVariableList;
        }

        internal List<GremlinVariable> Select(string label, GremlinVariable stopVariable = null)
        {
            List<GremlinVariable> taggedVariableList = ParentContext?.SelectParent(label, HomeVariable);
            if (taggedVariableList == null) taggedVariableList = new List<GremlinVariable>();

            var stopIndex = stopVariable == null ? VariableList.Count : VariableList.IndexOf(stopVariable);

            for (var i = 0; i < stopIndex; i++)
            {
                if (VariableList[i].Labels.Contains(label))
                {
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
                            if (subContextVar is GremlinGhostVariable)
                            {
                                var ghostVar = subContextVar as GremlinGhostVariable;
                                var newGhostVar = GremlinGhostVariable.Create(ghostVar.RealVariable,
                                    ghostVar.AttachedVariable, label);
                                taggedVariableList.Add(newGhostVar);
                            }
                            else
                            {
                                GremlinGhostVariable newVariable = GremlinGhostVariable.Create(subContextVar,
                                    VariableList[i], label);
                                taggedVariableList.Add(newVariable);
                            }
                        }
                    }
                }
            }
            return taggedVariableList;
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

        internal void SetPivotVariable(GremlinVariable newPivotVariable)
        {
            PivotVariable = newPivotVariable;
            if (PivotVariable is GremlinContextVariable)
            {
                //Ignore the inherited variable
                if (!(PivotVariable as GremlinContextVariable).IsFromSelect) return;
            }
            StepList.Add(newPivotVariable);
            newPivotVariable.HomeContext = this;
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

        internal bool IsVariableInCurrentContext(GremlinVariable variable)
        {
            return TableReferences.Contains(variable);
        }

        internal GremlinMatchPath GetPathFromPathList(GremlinVariable edge)
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
            AddPredicate(SqlUtil.GetBooleanParenthesisExpr(SqlUtil.ConcatBooleanExprWithOr(booleanExprList)));
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
            //generate tableReference in a reverse way, because the later tableReference may use the column of previous tableReference
            List<WTableReference> reversedTableReference = new List<WTableReference>();
            for (var i = TableReferences.Count - 1; i >= 0; i--)
            {
                reversedTableReference.Add(TableReferences[i].ToTableReference());
            }
            for (var i = reversedTableReference.Count - 1; i >= 0; i--)
            {
                newFromClause.TableReferences.Add(reversedTableReference[i]);
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

            if (PivotVariable is GremlinDropVariable
                || (PivotVariable is GremlinUnionVariable && HomeVariable is GremlinSideEffectVariable))
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetStarColumnReferenceExpr()));
            }

            //else if (PivotVariable.GetVariableType() == GremlinVariableType.Table)
            //{
            //    throw new Exception("Can't process table type");
            //}
            else
            {
                GremlinVariableProperty defaultProjection = PivotVariable.DefaultProjection();
                selectElements.Add(SqlUtil.GetSelectScalarExpr(defaultProjection.ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
            }

            if (ProjectedProperties != null && ProjectedProperties.Count != 0)
            {
                foreach (var projectProperty in ProjectedProperties)
                {
                    if (ProjectVariablePropertiesList.All(p => p.Item2 != projectProperty))
                    {
                        selectElements.Add(
                            SqlUtil.GetSelectScalarExpr(
                                PivotVariable.GetVariableProperty(projectProperty).ToScalarExpression(), projectProperty));
                    }
                }
            }
            if (isPopulateGremlinPath)
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(CurrentContextPath.DefaultProjection().ToScalarExpression(), GremlinKeyword.Path));
            }
            foreach (var item in ProjectVariablePropertiesList)
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(item.Item1.ToScalarExpression(), item.Item2));
            }
            if (selectElements.Count == 0)
            {
                if (PivotVariable is GremlinTableVariable
                    || (PivotVariable is GremlinUnionVariable && HomeVariable is GremlinSideEffectVariable))
                {
                    selectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetStarColumnReferenceExpr()));
                }
                else if (PivotVariable.GetVariableType() == GremlinVariableType.Table)
                {
                    throw new Exception("Can't process table type");
                }
                else
                {
                    GremlinVariableProperty defaultProjection = PivotVariable.DefaultProjection();
                    selectElements.Add(SqlUtil.GetSelectScalarExpr(defaultProjection.ToScalarExpression()));
                }
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


        internal void Both(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = lastVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = lastVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty,
                                                                             adjEdge,
                                                                             adjReverseEdge,
                                                                             labelProperty,
                                                                             WEdgeType.BothEdge);
            VariableList.Add(bothEdge);
            TableReferences.Add(bothEdge);
            AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinVariableProperty otherProperty = bothEdge.GetVariableProperty(GremlinKeyword.EdgeOtherV);
            GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(bothEdge.GetEdgeType(), otherProperty);
            VariableList.Add(otherVertex);
            TableReferences.Add(otherVertex);
            SetPivotVariable(otherVertex);
        }

        internal void BothE(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = lastVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = lastVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, labelProperty,
                WEdgeType.BothEdge);
            VariableList.Add(bothEdge);
            TableReferences.Add(bothEdge);
            AddLabelPredicateForEdge(bothEdge, edgeLabels);

            SetPivotVariable(bothEdge);
        }

        internal void BothV(GremlinVariable lastVariable)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinVariableProperty sinkProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(lastVariable.GetEdgeType(), sourceProperty, sinkProperty);

            VariableList.Add(bothVertex);
            TableReferences.Add(bothVertex);
            SetPivotVariable(bothVertex);
        }

        internal void In(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = lastVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge,
                labelProperty, WEdgeType.InEdge);
            VariableList.Add(inEdge);
            TableReferences.Add(inEdge);
            AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinVariableProperty edgeProperty = inEdge.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(inEdge.GetEdgeType(), edgeProperty);
            VariableList.Add(outVertex);
            TableReferences.Add(outVertex);

            AddPath(new GremlinMatchPath(outVertex, inEdge, lastVariable));

            SetPivotVariable(outVertex);
        }

        internal void InE(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = lastVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, labelProperty, WEdgeType.InEdge);
            VariableList.Add(inEdge);
            TableReferences.Add(inEdge);
            AddLabelPredicateForEdge(inEdge, edgeLabels);

            AddPath(new GremlinMatchPath(null, inEdge, lastVariable));
            SetPivotVariable(inEdge);
        }

        internal void Out(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = lastVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            VariableList.Add(outEdge);
            TableReferences.Add(outEdge);
            AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinVariableProperty sinkProperty = outEdge.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(outEdge.GetEdgeType(), sinkProperty);
            VariableList.Add(inVertex);
            TableReferences.Add(inVertex);

            AddPath(new GremlinMatchPath(lastVariable, outEdge, inVertex));

            SetPivotVariable(inVertex);
        }

        internal void OutE(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = lastVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            VariableList.Add(outEdge);
            TableReferences.Add(outEdge);
            AddLabelPredicateForEdge(outEdge, edgeLabels);

            AddPath(new GremlinMatchPath(lastVariable, outEdge, null));
            SetPivotVariable(outEdge);
        }

        internal void InV(GremlinVariable lastVariable)
        {
            switch (lastVariable.GetEdgeType())
            {
                case WEdgeType.BothEdge:
                    GremlinVariableProperty sinkProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
                    GremlinTableVariable inVertex = new GremlinBoundVertexVariable(lastVariable.GetEdgeType(), sinkProperty);
                    VariableList.Add(inVertex);
                    TableReferences.Add(inVertex);
                    SetPivotVariable(inVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    var path = GetPathFromPathList(lastVariable);
                    if (path != null && path.SinkVariable != null)
                    {
                        if (IsVariableInCurrentContext(path.SinkVariable))
                        {
                            SetPivotVariable(path.SinkVariable);
                        }
                        else
                        {
                            GremlinContextVariable newContextVariable = GremlinContextVariable.Create(path.SinkVariable);
                            VariableList.Add(newContextVariable);
                            SetPivotVariable(newContextVariable);
                        }
                    }
                    else
                    {
                        sinkProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
                        inVertex = lastVariable.CreateAdjVertex(sinkProperty);
                        if (path != null) path.SetSinkVariable(inVertex);

                        VariableList.Add(inVertex);
                        TableReferences.Add(inVertex);
                        SetPivotVariable(inVertex);
                    }
                    break;
            }
        }

        internal void OutV(GremlinVariable lastVariable)
        {
            switch (lastVariable.GetEdgeType())
            {
                case WEdgeType.BothEdge:
                    GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
                    GremlinTableVariable outVertex = new GremlinBoundVertexVariable(lastVariable.GetEdgeType(), sourceProperty);
                    VariableList.Add(outVertex);
                    TableReferences.Add(outVertex);
                    SetPivotVariable(outVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    var path = GetPathFromPathList(lastVariable);

                    if (path != null && path.SourceVariable != null)
                    {
                        if (IsVariableInCurrentContext(path.SourceVariable))
                        {
                            SetPivotVariable(path.SourceVariable);
                        }
                        else
                        {
                            GremlinContextVariable newContextVariable = GremlinContextVariable.Create(path.SourceVariable);
                            VariableList.Add(newContextVariable);
                            SetPivotVariable(newContextVariable);
                        }
                    }
                    else
                    {
                        sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
                        outVertex = lastVariable.CreateAdjVertex(sourceProperty);
                        if (path != null) path.SetSourceVariable(outVertex);

                        VariableList.Add(outVertex);
                        TableReferences.Add(outVertex);
                        SetPivotVariable(outVertex);
                    }
                    break;
            }
        }

        internal void OtherV(GremlinVariable lastVariable)
        {
            switch (lastVariable.GetEdgeType())
            {
                case WEdgeType.BothEdge:
                    GremlinVariableProperty otherProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeOtherV);
                    GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(lastVariable.GetEdgeType(), otherProperty);
                    VariableList.Add(otherVertex);
                    TableReferences.Add(otherVertex);
                    SetPivotVariable(otherVertex);
                    break;
                case WEdgeType.InEdge:
                    OutV(lastVariable);
                    break;
                case WEdgeType.OutEdge:
                    InV(lastVariable);
                    break;
            }
        }
    }
}
