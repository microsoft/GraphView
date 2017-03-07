using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinKeyVariable : GremlinScalarTableVariable
    {
        public GremlinVariableProperty ProjectVariable { get; set; }

        public GremlinKeyVariable(GremlinVariableProperty projectVariable)
        {
            ProjectVariable = projectVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(ProjectVariable.ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Key, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
