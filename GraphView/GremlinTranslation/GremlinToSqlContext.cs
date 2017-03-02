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
        internal List<GremlinTableVariable> TableReferences { get; private set; }
        internal WBooleanExpression Predicates { get; private set; }
        internal GremlinPathVariable CurrentContextPath { get; set; }
        internal List<Tuple<GremlinVariableProperty, string>> ProjectVariablePropertiesList { get; set; }
        internal List<string> ProjectedProperties { get; set; }

        internal bool IsPopulateGremlinPath;

        internal GremlinToSqlContext()
        {
            TableReferences = new List<GremlinTableVariable>();
            VariableList = new List<GremlinVariable>();
            PathList = new List<GremlinMatchPath>();
            StepList = new List<GremlinVariable>();
            IsPopulateGremlinPath = false;
            ProjectVariablePropertiesList = new List<Tuple<GremlinVariableProperty, string>>();
            ProjectedProperties = new List<string>();
        }

        internal GremlinToSqlContext Duplicate()
        {
            return new GremlinToSqlContext()
            {
                VariableList = new List<GremlinVariable>(this.VariableList),
                PivotVariable = this.PivotVariable,
                TableReferences = new List<GremlinTableVariable>(this.TableReferences),
                PathList = new List<GremlinMatchPath>(this.PathList),
                Predicates = this.Predicates,
                StepList = new List<GremlinVariable>(this.StepList),
                IsPopulateGremlinPath = this.IsPopulateGremlinPath,
                CurrentContextPath = this.CurrentContextPath,
                ProjectVariablePropertiesList = new List<Tuple<GremlinVariableProperty, string>>(this.ProjectVariablePropertiesList),
                ProjectedProperties = new List<string>(this.ProjectedProperties)
            };
        }

        internal void Reset()
        {
            PivotVariable = null;
            Predicates = null;
            VariableList.Clear();
            TableReferences.Clear();
            PathList.Clear();
            StepList.Clear();
            IsPopulateGremlinPath = false;
            CurrentContextPath = null;
            ProjectVariablePropertiesList.Clear();
            ProjectedProperties.Clear();
        }

        internal void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            ProjectedProperties.Add(property);
            PivotVariable.Populate(property);
        }

        internal void PopulateColumn(GremlinVariableProperty variableProperty, string alias)
        {
            if (ProjectedProperties.Contains(alias)) return;
            ProjectedProperties.Add(alias);
            ProjectVariablePropertiesList.Add(new Tuple<GremlinVariableProperty, string>(variableProperty, alias));
        }

        internal List<GremlinVariable> SelectVarsFromCurrAndChildContext(string label)
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

        internal List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            for (var i = 0; i < VariableList.Count; i++)
            {
                variableList.Add(VariableList[i]);
                List<GremlinVariable> subContextVariableList = VariableList[i].FetchVarsFromCurrAndChildContext();
                if (subContextVariableList != null)
                {
                    variableList.AddRange(subContextVariableList);
                }
            }
            return variableList;
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
            if (IsPopulateGremlinPath) return;

            GremlinPathVariable newVariable = new GremlinPathVariable(GetCurrAndChildGremlinStepList());
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            CurrentContextPath = newVariable;

            IsPopulateGremlinPath = true;
        }

        internal void SetPivotVariable(GremlinVariable newPivotVariable)
        {
            PivotVariable = newPivotVariable;
            StepList.Add(newPivotVariable);
            newPivotVariable.HomeContext = this;
        }

        internal List<GremlinVariableProperty> GetGremlinStepList(GremlinVariable stopVariable = null)
        {
            List<GremlinVariableProperty> gremlinStepList = ParentContext?.GetGremlinStepList(HomeVariable);
            if (gremlinStepList == null)
            {
                gremlinStepList = new List<GremlinVariableProperty>();
            }
            foreach (var step in StepList)
            {
                if (step == stopVariable) break;
                step.PopulateGremlinPath();
                gremlinStepList.Add(step.GetPath());
            }
            return gremlinStepList;
        }

        internal List<GremlinVariableProperty> GetCurrAndChildGremlinStepList(GremlinVariable stopVariable = null)
        {
            List<GremlinVariableProperty> gremlinStepList = new List<GremlinVariableProperty>();
            foreach (var step in StepList)
            {
                if (step == stopVariable) break;
                step.PopulateGremlinPath();
                gremlinStepList.Add(step.GetPath());
            }
            return gremlinStepList;
        }

        internal void AddPath(GremlinMatchPath path)
        {
            PathList.Add(path);
        }

        internal bool IsVariableInCurrentContext(GremlinVariable variable)
        {
            return TableReferences.Contains(variable);
        }

        internal GremlinMatchPath GetPathFromPathList(GremlinVariable edge)
        {
            return PathList.Find(p => p.EdgeVariable.GetVariableName() == edge.GetVariableName());
        }

        internal void AddPredicate(WBooleanExpression newPredicate)
        {
            if (newPredicate == null) return;
            Predicates = Predicates == null ? newPredicate : SqlUtil.GetAndBooleanBinaryExpr(Predicates, newPredicate);
        }

        internal void AddLabelPredicateForEdge(GremlinEdgeTableVariable edgeTable, List<string> edgeLabels)
        {
            if (edgeLabels.Count == 0) return;
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = edgeTable.GetVariableProperty(GremlinKeyword.Label).ToScalarExpression();
                var secondExpr = SqlUtil.GetValueExpr(edgeLabel);
                booleanExprList.Add(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            AddPredicate(SqlUtil.GetBooleanParenthesisExpr(SqlUtil.ConcatBooleanExprWithOr(booleanExprList)));
        }

        internal WBooleanExpression ToSqlBoolean()
        {
            if (TableReferences.Count == 0)
            {
                if (Predicates != null)
                {
                    return SqlUtil.GetBooleanParenthesisExpr(Predicates);
                }
                return null;
            }
            else
            {
                 return SqlUtil.GetExistPredicate(ToSelectQueryBlock());
            }
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
            foreach (var path in PathList)
            {
                if (path.EdgeVariable is GremlinFreeEdgeTableVariable && VariableList.Contains(path.EdgeVariable))
                {
                    newMatchClause.Paths.Add(path.ToMatchPath());
                }
            }
            return newMatchClause.Paths.Count == 0 ? null : newMatchClause;
        }

        internal List<WSelectElement> GetSelectElement(List<string> ProjectedProperties)
        {
            var selectElements = new List<WSelectElement>();

            if ((PivotVariable is GremlinUnionVariable && HomeVariable is GremlinSideEffectVariable)
                || PivotVariable.GetVariableType() == GremlinVariableType.NULL)
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetStarColumnReferenceExpr()));
            }
            else if (ProjectedProperties != null && ProjectedProperties.Count != 0)
            {
                foreach (var projectProperty in ProjectedProperties)
                {
                    WSelectScalarExpression selectScalarExpr;
                    if (projectProperty == GremlinKeyword.TableDefaultColumnName)
                    {
                        GremlinVariableProperty defaultProjection = PivotVariable.DefaultProjection();
                        selectScalarExpr = SqlUtil.GetSelectScalarExpr(defaultProjection.ToScalarExpression(), GremlinKeyword.TableDefaultColumnName);
                        selectElements.Add(selectScalarExpr);
                    }
                    else if (ProjectVariablePropertiesList.All(p => p.Item2 != projectProperty))
                    {
                        
                        if (PivotVariable.ProjectedProperties.Contains(projectProperty))
                        {
                            WScalarExpression columnExpr =
                                PivotVariable.GetVariableProperty(projectProperty).ToScalarExpression();
                            selectScalarExpr = SqlUtil.GetSelectScalarExpr(columnExpr, projectProperty);
                            selectElements.Add(selectScalarExpr);
                        }
                        else
                        {
                            selectScalarExpr = SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), projectProperty);
                            selectElements.Add(selectScalarExpr);
                        }
                    }
                }
            }
            else
            {
                GremlinVariableProperty defaultProjection = PivotVariable.DefaultProjection();
                selectElements.Add(SqlUtil.GetSelectScalarExpr(defaultProjection.ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
            }

            if (IsPopulateGremlinPath)
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
        }

        internal WGroupByClause GetGroupByClause()
        {
            return null;
        }

        internal void Both(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = lastVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = lastVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable bothEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty,
                                                                             adjEdge,
                                                                             adjReverseEdge,
                                                                             labelProperty,
                                                                             WEdgeType.BothEdge);
            VariableList.Add(bothEdgeTable);
            TableReferences.Add(bothEdgeTable);
            AddLabelPredicateForEdge(bothEdgeTable, edgeLabels);

            GremlinVariableProperty otherProperty = bothEdgeTable.GetVariableProperty(GremlinKeyword.EdgeOtherV);
            GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(bothEdgeTable.GetEdgeType(), otherProperty);
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
            GremlinBoundEdgeTableVariable bothEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjEdge, adjReverseEdge, labelProperty,
                WEdgeType.BothEdge);
            VariableList.Add(bothEdgeTable);
            TableReferences.Add(bothEdgeTable);
            AddLabelPredicateForEdge(bothEdgeTable, edgeLabels);

            SetPivotVariable(bothEdgeTable);
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
            GremlinBoundEdgeTableVariable inEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjReverseEdge,
                labelProperty, WEdgeType.InEdge);
            VariableList.Add(inEdgeTable);
            TableReferences.Add(inEdgeTable);
            AddLabelPredicateForEdge(inEdgeTable, edgeLabels);

            GremlinVariableProperty edgeProperty = inEdgeTable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(inEdgeTable.GetEdgeType(), edgeProperty);
            VariableList.Add(outVertex);
            TableReferences.Add(outVertex);

            AddPath(new GremlinMatchPath(outVertex, inEdgeTable, lastVariable));

            SetPivotVariable(outVertex);
        }

        internal void InE(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = lastVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable inEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjReverseEdge, labelProperty, WEdgeType.InEdge);
            VariableList.Add(inEdgeTable);
            TableReferences.Add(inEdgeTable);
            AddLabelPredicateForEdge(inEdgeTable, edgeLabels);

            AddPath(new GremlinMatchPath(null, inEdgeTable, lastVariable));
            SetPivotVariable(inEdgeTable);
        }

        internal void Out(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = lastVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable outEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            VariableList.Add(outEdgeTable);
            TableReferences.Add(outEdgeTable);
            AddLabelPredicateForEdge(outEdgeTable, edgeLabels);

            GremlinVariableProperty sinkProperty = outEdgeTable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(outEdgeTable.GetEdgeType(), sinkProperty);
            VariableList.Add(inVertex);
            TableReferences.Add(inVertex);

            AddPath(new GremlinMatchPath(lastVariable, outEdgeTable, inVertex));

            SetPivotVariable(inVertex);
        }

        internal void OutE(GremlinVariable lastVariable, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = lastVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = lastVariable.GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable outEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            VariableList.Add(outEdgeTable);
            TableReferences.Add(outEdgeTable);
            AddLabelPredicateForEdge(outEdgeTable, edgeLabels);

            AddPath(new GremlinMatchPath(lastVariable, outEdgeTable, null));
            SetPivotVariable(outEdgeTable);
        }

        internal void InV(GremlinVariable lastVariable)
        {
            if (lastVariable is GremlinFreeEdgeTableVariable)
            {
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
                    GremlinVariableProperty sinkProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
                    GremlinTableVariable inVertex = lastVariable.CreateAdjVertex(sinkProperty);
                    if (path != null) path.SetSinkVariable(inVertex);

                    VariableList.Add(inVertex);
                    TableReferences.Add(inVertex);
                    SetPivotVariable(inVertex);
                }
            }
            else
            {
                GremlinVariableProperty sinkProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
                GremlinTableVariable inVertex = new GremlinBoundVertexVariable(lastVariable.GetEdgeType(), sinkProperty);
                VariableList.Add(inVertex);
                TableReferences.Add(inVertex);
                SetPivotVariable(inVertex);
            }
        }

        internal void OutV(GremlinVariable lastVariable)
        {
            if (lastVariable is GremlinFreeEdgeTableVariable)
            {
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
                    GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
                    GremlinTableVariable outVertex = lastVariable.CreateAdjVertex(sourceProperty);
                    if (path != null) path.SetSourceVariable(outVertex);

                    VariableList.Add(outVertex);
                    TableReferences.Add(outVertex);
                    SetPivotVariable(outVertex);
                }
            }
            else
            {
                GremlinVariableProperty sourceProperty = lastVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
                GremlinTableVariable outVertex = new GremlinBoundVertexVariable(lastVariable.GetEdgeType(), sourceProperty);
                VariableList.Add(outVertex);
                TableReferences.Add(outVertex);
                SetPivotVariable(outVertex);
            }
        }

        internal void OtherV(GremlinVariable lastVariable)
        {
            switch (lastVariable.GetEdgeType())
            {
                case WEdgeType.Undefined:
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

        internal void Key(GremlinVariable lastVariable)
        {
            GremlinKeyVariable newVariable = new GremlinKeyVariable(lastVariable.DefaultVariableProperty());
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            SetPivotVariable(newVariable);
        }

        internal void Value(GremlinVariable lastVariable)
        {
            GremlinValueVariable newVariable = new GremlinValueVariable(lastVariable.DefaultVariableProperty());
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            SetPivotVariable(newVariable);
        }

        internal void DropProperties(GremlinVariable belongToVariable, List<string> PropertyKeys)
        {
            List<object> properties = new List<object>();
            foreach (var propertyKey in PropertyKeys)
            {
                properties.Add(propertyKey);
                properties.Add(null);
            }
            if (PropertyKeys.Count == 0)
            {
                properties.Add(GremlinKeyword.Star);
                properties.Add(null);
            }

            GremlinUpdatePropertiesVariable dropVariable = null;
            switch (belongToVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    dropVariable = new GremlinUpdateVertexPropertiesVariable(belongToVariable, properties);
                    break;
                case GremlinVariableType.Edge:
                    dropVariable = new GremlinUpdateEdgePropertiesVariable(belongToVariable, properties);
                    break;
            }

            VariableList.Add(dropVariable);
            TableReferences.Add(dropVariable);
            SetPivotVariable(dropVariable);
        }

        internal void DropVertex(GremlinVariable dropVertexVariable)
        {
            GremlinVariableProperty variableProperty = dropVertexVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinDropVariable dropVariable = new GremlinDropVertexVariable(variableProperty);
            VariableList.Add(dropVariable);
            TableReferences.Add(dropVariable);
            SetPivotVariable(dropVariable);
        }

        internal void DropEdge(GremlinVariable dropEdgeVariable)
        {
            var sourceProperty = dropEdgeVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            var edgeProperty = dropEdgeVariable.GetVariableProperty(GremlinKeyword.EdgeID);
            GremlinDropVariable dropVariable = new GremlinDropEdgeVariable(sourceProperty, edgeProperty);
            VariableList.Add(dropVariable);
            TableReferences.Add(dropVariable);
            SetPivotVariable(dropVariable);
        }

        internal void Has(GremlinVariable lastVariable, string propertyKey)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(lastVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(propertyKey));
            AddPredicate(SqlUtil.GetFunctionBooleanExpression("hasProperty", parameters));
        }

        internal void Has(GremlinVariable lastVariable, string propertyKey, object value)
        {
            WScalarExpression firstExpr = lastVariable.GetVariableProperty(propertyKey).ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
            AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        internal void Has(GremlinVariable lastVariable, string label, string propertyKey, object value)
        {
            Has(lastVariable, GremlinKeyword.Label, label);
            Has(lastVariable, propertyKey, value);
        }

        internal void Has(GremlinVariable lastVariable, string propertyKey, Predicate predicate)
        {
            WScalarExpression firstExpr = lastVariable.GetVariableProperty(propertyKey).ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(predicate.Value);
            AddPredicate(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate));
        }

        internal void Has(GremlinVariable lastVariable, string label, string propertyKey, Predicate predicate)
        {
            Has(lastVariable, GremlinKeyword.Label, label);
            Has(lastVariable, propertyKey, predicate);
        }

        internal void Has(GremlinVariable lastVariable, string propertyKey, GremlinToSqlContext propertyContext)
        {
            Has(lastVariable, propertyKey);
            AddPredicate(propertyContext.ToSqlBoolean());
        }

        internal void HasId(GremlinVariable lastVariable, List<object> values)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = lastVariable.GetVariableProperty(lastVariable.GetPrimaryKey()).ToScalarExpression();
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
                booleanExprList.Add(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, BooleanComparisonType.Equals));
            }
            WBooleanExpression concatSql = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
            AddPredicate(SqlUtil.GetBooleanParenthesisExpr(concatSql));
        }

        internal void HasId(GremlinVariable lastVariable, Predicate predicate)
        {
            WScalarExpression firstExpr = lastVariable.GetVariableProperty(lastVariable.GetPrimaryKey()).ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(predicate.Value);
            AddPredicate(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, BooleanComparisonType.Equals));
        }

        internal void HasLabel(GremlinVariable lastVariable, List<object> values)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = lastVariable.GetVariableProperty(GremlinKeyword.Label).ToScalarExpression();
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
                booleanExprList.Add(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            WBooleanExpression concatSql = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
            AddPredicate(SqlUtil.GetBooleanParenthesisExpr(concatSql));
        }

        internal void HasLabel(GremlinVariable lastVariable, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(propertyKey, traversal) step only applies to vertices and edges.");
        }

        internal void HasKey(GremlinVariable lastVariable, List<string> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal void HasKey(GremlinVariable lastVariable, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal void HasValue(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal void HasValue(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal void HasNot(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new QueryCompilationException("The HasNot(propertyKey) step only applies to vertices and edges.");
        }

        internal void Properties(GremlinVariable lastVariable, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 0)
            {
                lastVariable.Populate(GremlinKeyword.Star);
            }
            else
            {
                foreach (var property in propertyKeys)
                {
                    lastVariable.Populate(property);
                }
            }
            GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(lastVariable, propertyKeys);
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            SetPivotVariable(newVariable);
        }

        internal void Values(GremlinVariable lastVariable, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 0)
            {
                lastVariable.Populate(GremlinKeyword.Star);
            }
            else
            {
                foreach (var property in propertyKeys)
                {
                    lastVariable.Populate(property);
                }
            }
            GremlinValuesVariable newVariable = new GremlinValuesVariable(lastVariable, propertyKeys);
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            SetPivotVariable(newVariable);
        }
    }
}
