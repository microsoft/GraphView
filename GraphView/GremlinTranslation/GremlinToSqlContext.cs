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
            GremlinVariable inputVariable = null;
            if (VariableList.First() is GremlinContextVariable)
            {
                inputVariable = VariableList.First();
            }

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

            //reserve the InputVariable
            if (inputVariable != null)
            {
                VariableList.Add(inputVariable);
            }
        }

        internal void Populate(string property)
        {
            if (ProjectedProperties.Contains(property) || property == GremlinKeyword.TableDefaultColumnName) return;
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

        internal List<GremlinVariable> Select(string label, GremlinVariable stopVariable = null)
        {
            List<GremlinVariable> taggedVariableList = ParentContext?.Select(label, HomeVariable);
            if (taggedVariableList == null) taggedVariableList = new List<GremlinVariable>();

            var stopIndex = stopVariable == null ? VariableList.Count : VariableList.IndexOf(stopVariable);

            for (var i = 0; i < stopIndex; i++)
            {
                if (VariableList[i].Labels.Contains(label))
                {
                    taggedVariableList.Add(new GremlinContextVariable(VariableList[i]));
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

        internal GremlinPathVariable PopulateGremlinPath()
        {
            if (IsPopulateGremlinPath) return CurrentContextPath;

            GremlinPathVariable newVariable = new GremlinPathVariable(GetCurrAndChildGremlinStepList());
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            CurrentContextPath = newVariable;

            IsPopulateGremlinPath = true;

            return newVariable;
        }

        internal void SetPivotVariable(GremlinVariable newPivotVariable)
        {
            PivotVariable = newPivotVariable;
            StepList.Add(newPivotVariable);
            newPivotVariable.HomeContext = this;
        }

        internal List<GremlinPathStepVariable> GetGremlinStepList(GremlinVariable stopVariable = null)
        {
            List<GremlinPathStepVariable> gremlinStepList = ParentContext?.GetGremlinStepList(HomeVariable);
            if (gremlinStepList == null)
            {
                gremlinStepList = new List<GremlinPathStepVariable>();
            }
            foreach (var step in StepList)
            {
                if (step == stopVariable) break;
                gremlinStepList.Add(step.GetAndPopulatePath());
            }
            return gremlinStepList;
        }

        internal List<GremlinPathStepVariable> GetCurrAndChildGremlinStepList(GremlinVariable stopVariable = null)
        {
            List<GremlinPathStepVariable> gremlinStepList = new List<GremlinPathStepVariable>();
            foreach (var step in StepList)
            {
                if (step == stopVariable) break;
                gremlinStepList.Add(step.GetAndPopulatePath());
            }
            return gremlinStepList;
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
                    return Predicates;
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
                WhereClause = GetWhereClause()
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
                 newMatchClause.Paths.Add(SqlUtil.GetMatchPath(path));
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
                return selectElements;
            }
            if (ProjectedProperties != null && ProjectedProperties.Count != 0)
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
                    else
                    if (ProjectVariablePropertiesList.All(p => p.Item2 != projectProperty))
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
                selectElements.Add(SqlUtil.GetSelectScalarExpr(PivotVariable.DefaultProjection().ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
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
    }
}
