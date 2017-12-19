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
        internal HashSet<string> ProjectedProperties { get; set; }  // Used for generating select clause
        internal List<GremlinTableVariable> TableReferencesInFromClause { get; private set; } // Used for generating from clause
        internal List<GremlinMatchPath> MatchPathList { get; set; }  // Used for generating match clause
        internal WBooleanExpression Predicates { get; private set; } // Used for generating where clause
        internal List<GremlinTableVariable> AllTableVariablesInWhereClause { get; private set; } // Store all tableVariables in where clause (this.Predicates)
        internal List<GremlinVariable> StepList { get; set; }  // Used for generating Path
        internal GremlinLocalPathVariable ContextLocalPath { get; set; }
        public int MinPathLength { get; set; }

        internal GremlinToSqlContext()
        {
            this.TableReferencesInFromClause = new List<GremlinTableVariable>();
            this.AllTableVariablesInWhereClause = new List<GremlinTableVariable>();
            this.VariableList = new List<GremlinVariable>();
            this.MatchPathList = new List<GremlinMatchPath>();
            this.StepList = new List<GremlinVariable>();
            this.ProjectedProperties = new HashSet<string>();
        }

        internal GremlinToSqlContext Duplicate()
        {
            return new GremlinToSqlContext()
            {
                ParentContext = this.ParentContext,
                PivotVariable = this.PivotVariable,
                VariableList = new List<GremlinVariable>(this.VariableList),
                ProjectedProperties = new HashSet<string>(this.ProjectedProperties),
                TableReferencesInFromClause = new List<GremlinTableVariable>(this.TableReferencesInFromClause),
                AllTableVariablesInWhereClause = new List<GremlinTableVariable>(this.AllTableVariablesInWhereClause),
                MatchPathList = new List<GremlinMatchPath>(this.MatchPathList),
                Predicates = this.Predicates,
                StepList = new List<GremlinVariable>(this.StepList),
                ContextLocalPath = this.ContextLocalPath,
            };
        }

        internal void Reset()
        {
            GremlinVariable inputVariable = null;
            if (this.VariableList.First() is GremlinContextVariable)
            {
                inputVariable = this.VariableList.First();
            }

            this.ParentContext = null;
            this.PivotVariable = null;
            this.VariableList.Clear();
            this.ProjectedProperties.Clear();
            this.TableReferencesInFromClause.Clear();
            this.AllTableVariablesInWhereClause.Clear();
            this.MatchPathList.Clear();
            this.Predicates = null;
            this.StepList.Clear();
            this.ContextLocalPath = null;

            //TODO: reserve the InputVariable, used for repeat step, should be refactored later
            if (inputVariable != null)
            {
                this.VariableList.Add(inputVariable);
            }
        }

        internal bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (this.PivotVariable.Populate(property, label))
            {
                populateSuccessfully = true;
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        internal void PopulateLocalPath()
        {
            if (this.ContextLocalPath != null)
            {
                return;
            }
            this.ProjectedProperties.Add(GremlinKeyword.Path);

            this.MinPathLength = 0;
            foreach (var step in this.StepList)
            {
                step.PopulateLocalPath();
                this.MinPathLength += step.LocalPathLengthLowerBound;
            }

            GremlinLocalPathVariable newVariable = new GremlinLocalPathVariable(this.StepList);
            this.VariableList.Add(newVariable);
            this.TableReferencesInFromClause.Add(newVariable);
            this.ContextLocalPath = newVariable;
        }

        internal List<GremlinVariable> PopulateGlobalPathStepList()
        {
            List<GremlinVariable> steps = this.ParentContext?.PopulateGlobalPathStepList() ?? new List<GremlinVariable>();
            foreach (var step in this.StepList)
            {
                step.PopulateLocalPath();
            }
            steps.AddRange(this.StepList);
            return steps;
        }

        internal List<GremlinVariable> GetSideEffectVariables()
        {
            List<GremlinVariable> sideEffectVariables = this.ParentContext?.GetSideEffectVariables() ?? new List<GremlinVariable>();
            foreach (var variable in this.VariableList)
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

        internal List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable>();
            this.TableReferencesInFromClause.ForEach(x => variableList.AddRange(x.FetchAllTableVars()));
            variableList.AddRange(this.AllTableVariablesInWhereClause);
            return variableList;
        }

        internal List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            this.VariableList.ForEach(x => variableList.AddRange(x.FetchAllVars()));
            return variableList;
        }

        internal void SetPivotVariable(GremlinVariable newPivotVariable)
        {
            this.PivotVariable = newPivotVariable;
            this.StepList.Add(newPivotVariable);
        }

        internal void AddPredicate(WBooleanExpression newPredicate)
        {
            if (newPredicate == null) return;
            this.Predicates = this.Predicates == null ? newPredicate : SqlUtil.GetAndBooleanBinaryExpr(this.Predicates, newPredicate);
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
            if (this.TableReferencesInFromClause.Count == 0)
            {
                return this.Predicates;
            }
            return SqlUtil.GetExistPredicate(this.ToSelectQueryBlock());
        }

        internal WSqlScript ToSqlScript()
        {
            return new WSqlScript() { Batches = this.GetBatchList() };
        }

        internal List<WSqlBatch> GetBatchList()
        {
            return new List<WSqlBatch>()
            {
                new WSqlBatch()
                {
                    Statements = this.GetStatements()
                }
            };
        }

        internal List<WSqlStatement> GetStatements()
        {
            return new List<WSqlStatement>() { this.ToSelectQueryBlock() };
        }

        internal WSelectQueryBlock ToSelectQueryBlock(bool isToCompose1 = false)
        {
            return new WSelectQueryBlock()
            {
                SelectElements = this.GetSelectElement(isToCompose1),
                FromClause = this.GetFromClause(),
                MatchClause = this.GetMatchClause(),
                WhereClause = this.GetWhereClause()
            };
        }

        internal List<WSelectElement> GetSelectElement(bool isToCompose1)
        {
            var selectElements = new List<WSelectElement>();

            if (this.PivotVariable.GetVariableType() == GremlinVariableType.NULL)
            {
                selectElements.Add(
                    SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), 
                    GremlinKeyword.TableDefaultColumnName));
                return selectElements;
            }

            if (isToCompose1)
            {
                selectElements.Add(
                    SqlUtil.GetSelectScalarExpr(this.PivotVariable.ToCompose1(), GremlinKeyword.TableDefaultColumnName));
                return selectElements;
            }

            GremlinVariableProperty defaultProjection = this.PivotVariable.DefaultProjection();
            selectElements.Add(
                SqlUtil.GetSelectScalarExpr(defaultProjection.ToScalarExpression(), this.PivotVariable.DefaultProperty()));

            foreach (var property in this.ProjectedProperties)
            {
                WScalarExpression selectScalarExpr;
                if (property == GremlinKeyword.Path)
                {
                    selectScalarExpr = ContextLocalPath.DefaultProjection().ToScalarExpression();
                }
                else
                {
                    selectScalarExpr = this.PivotVariable.ProjectedProperties.Contains(property)
                        ? this.PivotVariable.GetVariableProperty(property).ToScalarExpression()
                        : SqlUtil.GetValueExpr(null);
                }
                selectElements.Add(SqlUtil.GetSelectScalarExpr(selectScalarExpr, property));
            }

            return selectElements;
        }

        internal WFromClause GetFromClause()
        {
            if (this.TableReferencesInFromClause.Count == 0) return null;

            var newFromClause = new WFromClause();
            //generate tableReference in a reverse way, because the later tableReference may use the column of previous tableReference
            List<WTableReference> reversedTableReference = new List<WTableReference>();
            for (var i = this.TableReferencesInFromClause.Count - 1; i >= 0; i--)
            {
                reversedTableReference.Add(this.TableReferencesInFromClause[i].ToTableReference());
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
            foreach (var path in this.MatchPathList)
            {
                newMatchClause.Paths.Add(SqlUtil.GetMatchPath(path));
            }
            return newMatchClause.Paths.Count == 0 ? null : newMatchClause;
        }

        internal WWhereClause GetWhereClause()
        {
            return this.Predicates == null ? null : SqlUtil.GetWhereClause(this.Predicates);
        }
    }
}
