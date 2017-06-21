using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOptionalVariable : GremlinTableVariable
    {
        public GremlinToSqlContext OptionalContext { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinOptionalVariable(GremlinVariable inputVariable,
                                       GremlinToSqlContext context,
                                       GremlinVariableType variableType)
            : base(variableType)
        {
            OptionalContext = context;
            InputVariable = inputVariable;
        }

        internal override void PopulateStepProperty(string property)
        {
            OptionalContext.ContextLocalPath.PopulateStepProperty(property);
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path)) return;
            ProjectedProperties.Add(GremlinKeyword.Path);
            OptionalContext.PopulateLocalPath();
        }

        internal override void Populate(string property)
        {
            base.Populate(property);

            InputVariable.Populate(property);
            OptionalContext.Populate(property);
        }

        internal override WScalarExpression ToStepScalarExpr()
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(InputVariable);
            variableList.AddRange(OptionalContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(OptionalContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock firstQueryExpr = new WSelectQueryBlock();

            foreach (var projectProperty in ProjectedProperties)
            {
                if (projectProperty == GremlinKeyword.TableDefaultColumnName)
                {
                    firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(InputVariable.DefaultProjection().ToScalarExpression(),
                        GremlinKeyword.TableDefaultColumnName));
                }
                else if (InputVariable.ProjectedProperties.Contains(projectProperty))
                {
                    firstQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(
                            InputVariable.GetVariableProperty(projectProperty).ToScalarExpression(), projectProperty));
                }
                else
                {
                    firstQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), projectProperty));
                }
            }

            WSelectQueryBlock secondQueryExpr = OptionalContext.ToSelectQueryBlock();
            bool HasAggregateFunctionAsChildren = false;
            foreach (var variable in OptionalContext.TableReferencesInFromClause)
            {
                if (variable is GremlinFoldVariable
                    || variable is GremlinCountVariable
                    || variable is GremlinMinVariable
                    || variable is GremlinMaxVariable
                    || variable is GremlinSumVariable
                    || variable is GremlinMeanVariable
                    || variable is GremlinTreeVariable)
                {
                    HasAggregateFunctionAsChildren = true;
                }
                var group = variable as GremlinGroupVariable;
                if (group != null && group.SideEffectKey == null)
                {
                    HasAggregateFunctionAsChildren = true;
                }
            }

            var WBinaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, secondQueryExpr);

            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(WBinaryQueryExpression));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Optional, parameters, GetVariableName());
            ((WOptionalTableReference) tableRef).HasAggregateFunctionAsChildren = HasAggregateFunctionAsChildren;

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
