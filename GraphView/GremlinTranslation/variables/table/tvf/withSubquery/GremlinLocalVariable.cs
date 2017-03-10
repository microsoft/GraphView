using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalVariable : GremlinTableVariable
    {
        public GremlinToSqlContext LocalContext { get; set; }

        public GremlinLocalVariable(GremlinToSqlContext localContext, GremlinVariableType variableType)
            : base(variableType)
        {
            LocalContext = localContext;
            LocalContext.HomeVariable = this;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            LocalContext.Populate(property);
        }

        internal override GremlinPathStepVariable GetAndPopulatePath()
        {
            GremlinPathVariable pathVariable = LocalContext.PopulateGremlinPath();
            return new GremlinPathStepVariable(pathVariable, this);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return LocalContext == null ? new List<GremlinVariable>(): LocalContext.FetchVarsFromCurrAndChildContext();
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            return LocalContext.SelectVarsFromCurrAndChildContext(label);
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return LocalContext.PivotVariable.GetUnfoldVariableType();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(LocalContext.ToSelectQueryBlock(ProjectedProperties)));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Local, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
