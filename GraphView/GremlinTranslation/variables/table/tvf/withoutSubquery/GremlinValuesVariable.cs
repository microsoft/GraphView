using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinValuesVariable: GremlinScalarTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable ProjectVariable { get; set; }

        public GremlinValuesVariable(GremlinVariable projectVariable, List<string> propertyKeys)
        {
            ProjectVariable = projectVariable;
            PropertyKeys = new List<string>(propertyKeys);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (PropertyKeys.Count == 0)
            {
                parameters.Add(ProjectVariable.GetVariableProperty(GremlinKeyword.Star).ToScalarExpression());
            }
            else
            {
                foreach (var property in PropertyKeys)
                {
                    parameters.Add(ProjectVariable.GetVariableProperty(property).ToScalarExpression());
                }
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Values, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
