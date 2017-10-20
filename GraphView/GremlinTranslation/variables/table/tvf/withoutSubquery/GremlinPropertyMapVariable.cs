using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertyMapVariable : GremlinMapTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinPropertyMapVariable(GremlinVariable inputVariable, List<string> propertyKeys)
        {
            GremlinVariableType inputVariableType = inputVariable.GetVariableType();
            if (!(inputVariableType <= GremlinVariableType.Unknown))
            {
                throw new SyntaxErrorException("The inputVariable of propertyMap() can not be " + inputVariableType);
            }

            this.InputVariable = inputVariable;
            this.PropertyKeys = propertyKeys;
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
            parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.PropertyKeys.Select(SqlUtil.GetValueExpr));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.PropertyMap, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
