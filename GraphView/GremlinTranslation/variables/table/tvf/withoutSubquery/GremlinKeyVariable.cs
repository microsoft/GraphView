using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinKeyVariable : GremlinScalarTableVariable
    {
        public GremlinVariable ProjectVariable { get; set; }

        public GremlinKeyVariable(GremlinVariable projectVariable)
        {
            this.ProjectVariable = projectVariable;
        }

        internal override bool Populate(string property, string label = null)
        {
            return false;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.ProjectVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.ProjectVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Key, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
