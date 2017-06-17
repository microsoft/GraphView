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
        internal GremlinVariable PivotVariable { get; set; }
        internal List<GremlinVariable> VariableList { get; private set; }
        internal List<string> ProjectedProperties { get; set; }  // Used for generating select clause
        internal List<GremlinTableVariable> TableReferences { get; private set; } // Used for generating from clause
        internal List<GremlinMatchPath> MatchPathList { get; set; }  // Used for generating match clause
        internal WBooleanExpression Predicates { get; private set; } // Used for generating where clause
        internal bool HasRepeatPathInPredicates { get; set; }
        internal List<GremlinVariable> StepList { get; set; }  // Used for generating Path
        internal GremlinLocalPathVariable ContextLocalPath { get; set; }

        internal GremlinToSqlContext()
        {
            TableReferences = new List<GremlinTableVariable>();
            VariableList = new List<GremlinVariable>();
            MatchPathList = new List<GremlinMatchPath>();
            StepList = new List<GremlinVariable>();
            ProjectedProperties = new List<string>();
        }

        internal GremlinToSqlContext Duplicate()
        {
            return new GremlinToSqlContext()
            {
                ParentContext = this.ParentContext,
                PivotVariable = this.PivotVariable,
                VariableList = new List<GremlinVariable>(this.VariableList),
                ProjectedProperties = new List<string>(this.ProjectedProperties),
                TableReferences = new List<GremlinTableVariable>(this.TableReferences),
                MatchPathList = new List<GremlinMatchPath>(this.MatchPathList),
                Predicates = this.Predicates,
                StepList = new List<GremlinVariable>(this.StepList),
                ContextLocalPath = this.ContextLocalPath,
            };
        }

        internal void Reset()
        {
            GremlinVariable inputVariable = null;
            if (VariableList.First() is GremlinContextVariable)
            {
                inputVariable = VariableList.First();
            }

            ParentContext = null;
            PivotVariable = null;
            VariableList.Clear();
            ProjectedProperties.Clear();
            TableReferences.Clear();
            MatchPathList.Clear();
            Predicates = null;
            StepList.Clear();
            ContextLocalPath = null;

            //TODO: reserve the InputVariable, used for repeat step, should be refactored later
            if (inputVariable != null)
            {
                VariableList.Add(inputVariable);
            }
        }

        internal void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            ProjectedProperties.Add(property);
            PivotVariable.Populate(property);
        }

        internal void PopulateLocalPath()
        {
            if (ContextLocalPath != null) return;
            ProjectedProperties.Add(GremlinKeyword.Path);

            foreach (var step in StepList)
            {
                step.PopulateLocalPath();
            }

            GremlinLocalPathVariable newVariable = new GremlinLocalPathVariable(StepList);
            VariableList.Add(newVariable);
            TableReferences.Add(newVariable);
            ContextLocalPath = newVariable;
        }

        internal List<GremlinVariable> GetGlobalPathStepList()
        {
            List<GremlinVariable> steps = ParentContext?.GetGlobalPathStepList() ?? new List<GremlinVariable>();
            foreach (var step in StepList)
            {
                step.PopulateLocalPath();
            }
            steps.AddRange(StepList);
            return steps;
        }

        internal List<GremlinVariable> GetSideEffectVariables()
        {
            List<GremlinVariable> sideEffectVariables = ParentContext?.GetSideEffectVariables() ?? new List<GremlinVariable>();
            foreach (var variable in VariableList)
            {
                var aggregate = variable as GremlinAggregateVariable;
                if (aggregate != null)
                    sideEffectVariables.Add(variable);

                var store = variable as GremlinStoreVariable;
                if (store != null)
                    sideEffectVariables.Add(variable);

                var treeSideEffect = variable as GremlinTreeSideEffectVariable;
                if (treeSideEffect != null)
                    sideEffectVariables.Add(variable);

                var groupSideEffect = variable as GremlinGroupVariable;
                if (groupSideEffect != null && groupSideEffect.SideEffectKey != null)
                    sideEffectVariables.Add(variable);
            }
            
            return sideEffectVariables;
        }

        internal List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            for (var i = 0; i < TableReferences.Count; i++)
            {
                variableList.AddRange(TableReferences[i].FetchAllTableVars());
            }
            return variableList;
        }

        internal List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            for (var i = 0; i < VariableList.Count; i++)
            {
                variableList.AddRange(VariableList[i].FetchAllVars());
            }
            return variableList;
        }

        internal void SetPivotVariable(GremlinVariable newPivotVariable)
        {
            PivotVariable = newPivotVariable;
            StepList.Add(newPivotVariable);
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
            AddPredicate(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));
        }

        internal WBooleanExpression ToSqlBoolean()
        {
            if (TableReferences.Count == 0)
            {
                return Predicates;
            }
            this.HasRepeatPathInPredicates = this.FetchAllTableVars()
                .Exists(var => (var is GremlinGlobalPathVariable) && ((var as GremlinGlobalPathVariable).GetStepList()
                                   .Exists(step => step is GremlinRepeatContextVariable)));
            return SqlUtil.GetExistPredicate(ToSelectQueryBlock());
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

        internal WSelectQueryBlock ToSelectQueryBlock(bool isToCompose1 = false)
        {
            return new WSelectQueryBlock()
            {
                SelectElements = GetSelectElement(isToCompose1),
                FromClause = GetFromClause(),
                MatchClause = GetMatchClause(),
                WhereClause = GetWhereClause()
            };
        }

        internal List<WSelectElement> GetSelectElement(bool isToCompose1)
        {
            var selectElements = new List<WSelectElement>();

            if (PivotVariable.GetVariableType() == GremlinVariableType.NULL)
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetStarColumnReferenceExpr(), GremlinKeyword.TableDefaultColumnName));
                return selectElements;
            }

            if (isToCompose1)
            {
                selectElements.Add(SqlUtil.GetSelectScalarExpr(PivotVariable.ToCompose1(), GremlinKeyword.TableDefaultColumnName));
                return selectElements;
            }

            foreach (var projectProperty in ProjectedProperties)
            {
                WSelectScalarExpression selectScalarExpr;
                if (projectProperty == GremlinKeyword.Path)
                {
                    selectScalarExpr = SqlUtil.GetSelectScalarExpr(ContextLocalPath.DefaultProjection().ToScalarExpression(), GremlinKeyword.Path);
                }
                else if (projectProperty == GremlinKeyword.TableDefaultColumnName)
                {
                    GremlinVariableProperty defaultProjection = PivotVariable.DefaultProjection();
                    selectScalarExpr = SqlUtil.GetSelectScalarExpr(defaultProjection.ToScalarExpression(), GremlinKeyword.TableDefaultColumnName);
                }
                else if (PivotVariable.ProjectedProperties.Contains(projectProperty))
                {
                    WScalarExpression columnExpr = PivotVariable.GetVariableProperty(projectProperty).ToScalarExpression();
                    selectScalarExpr = SqlUtil.GetSelectScalarExpr(columnExpr, projectProperty);
                }
                else
                {
                    selectScalarExpr = SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), projectProperty);
                }
                selectElements.Add(selectScalarExpr);
            }

            if (selectElements.Count == 0)
            {
                GremlinVariableProperty defaultProjection = PivotVariable.DefaultProjection();
                selectElements.Add(SqlUtil.GetSelectScalarExpr(defaultProjection.ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
            }

            return selectElements;
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
            foreach (var path in MatchPathList)
            {
                newMatchClause.Paths.Add(SqlUtil.GetMatchPath(path));
            }
            return newMatchClause.Paths.Count == 0 ? null : newMatchClause;
        }

        internal WWhereClause GetWhereClause()
        {
            return Predicates == null ? null : SqlUtil.GetWhereClause(Predicates);
        }
    }
}
