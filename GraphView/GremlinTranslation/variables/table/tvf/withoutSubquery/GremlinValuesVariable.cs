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
            GremlinVariableType inputVariableType = projectVariable.GetVariableType();
            if (!(inputVariableType <= GremlinVariableType.Unknown))
            {
                throw new SyntaxErrorException("The inputVariable of values() can not be " + inputVariableType);
            }

            this.ProjectVariable = projectVariable;
            this.PropertyKeys = new List<string>(propertyKeys);
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
            WTableReference tableRef = null;
            if (this.PropertyKeys.Count == 0)
            {
                parameters.Add(this.ProjectVariable.DefaultProjection().ToScalarExpression());
                tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AllValues, parameters, GetVariableName());
            }
            else
            {
                parameters.AddRange(this.PropertyKeys.Select(property => this.ProjectVariable.GetVariableProperty(property).ToScalarExpression()));
                tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Values, parameters, GetVariableName());
            }
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
