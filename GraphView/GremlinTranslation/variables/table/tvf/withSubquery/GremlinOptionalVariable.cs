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
            OptionalContext.HomeVariable = this;
        }

        internal override GremlinPathStepVariable GetAndPopulatePath()
        {
            GremlinPathVariable pathVariable = OptionalContext.PopulateGremlinPath();
            return new GremlinPathStepVariable(pathVariable, this);
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            InputVariable.Populate(property);
            OptionalContext.Populate(property);
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            throw new NotImplementedException();
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            return OptionalContext.SelectVarsFromCurrAndChildContext(label);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return OptionalContext == null ? new List<GremlinVariable>() :OptionalContext.FetchVarsFromCurrAndChildContext();
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            foreach (var variable in OptionalContext.VariableList)
            {
                if (variable.ContainsLabel(label))
                {
                    return true;
                }
            }
            return false;
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
                else
                if (InputVariable.ProjectedProperties.Contains(projectProperty))
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

            if (OptionalContext.IsPopulateGremlinPath)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), GremlinKeyword.Path));
            }

            WSelectQueryBlock secondQueryExpr = OptionalContext.ToSelectQueryBlock(ProjectedProperties);
            var WBinaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, secondQueryExpr);

            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(WBinaryQueryExpression));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Optional, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
