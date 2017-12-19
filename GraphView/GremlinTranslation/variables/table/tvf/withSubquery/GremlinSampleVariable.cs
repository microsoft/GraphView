using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSampleVariable : GremlinTableVariable
    {
        public GremlinVariable InputVariable;
        public GremlinKeyword.Scope Scope { get; set; }
        public int AmountToSample { get; set; }
        public GremlinToSqlContext ProbabilityContext { get; set; }

        public GremlinSampleVariable(GremlinVariable inputVariable, GremlinKeyword.Scope scope, int amountToSample, 
            GremlinToSqlContext probabilityContext) : base(inputVariable.GetVariableType())
        {
            this.InputVariable = inputVariable;
            this.Scope = scope;
            this.AmountToSample = amountToSample;
            this.ProbabilityContext = probabilityContext;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.InputVariable.Populate(property, null);
            }
            else
            {
                populateSuccessfully |= this.InputVariable.Populate(property, label);
                return true;
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.ProbabilityContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.ProbabilityContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            if (this.Scope == GremlinKeyword.Scope.Local)
            {
                parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
                parameters.Add(SqlUtil.GetValueExpr(this.AmountToSample));
                parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
                var tableRef =
                    SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SampleLocal, parameters, GetVariableName());
                return SqlUtil.GetCrossApplyTableReference(tableRef);
            }
            else
            {
                parameters.Add(SqlUtil.GetValueExpr(this.AmountToSample));
                if (this.ProbabilityContext != null)
                {
                    parameters.Add(SqlUtil.GetScalarSubquery(this.ProbabilityContext.ToSelectQueryBlock()));
                }
                var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SampleGlobal, parameters, GetVariableName());
                return SqlUtil.GetCrossApplyTableReference(tableRef);
            }
        }
    }
}
