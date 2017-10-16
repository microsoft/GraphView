using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertiesVariable: GremlinVertexPropertyTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinPropertiesVariable(GremlinVariable inputVariable, List<string> propertyKeys)
        {
            GremlinVariableType inputVariableType = inputVariable.GetVariableType();
            if (!(inputVariableType <= GremlinVariableType.Unknown || inputVariableType == GremlinVariableType.VertexProperty))
            {
                throw new SyntaxErrorException("The inputVariable of properties() can not be " + inputVariableType);
            }

            this.InputVariable = inputVariable;
            this.PropertyKeys = new List<string>(propertyKeys);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.InputVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            bool isFetchAll = false;
            if (this.PropertyKeys.Count == 0)
            {
                parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
                isFetchAll = true;
            }
            else
            {
                parameters.AddRange(this.PropertyKeys.Select(property => this.InputVariable.GetVariableProperty(property).ToScalarExpression()));
            }
            
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));

            var tableRef =
                SqlUtil.GetFunctionTableReference(
                    isFetchAll ? GremlinKeyword.func.AllProperties : GremlinKeyword.func.Properties,
                    parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
