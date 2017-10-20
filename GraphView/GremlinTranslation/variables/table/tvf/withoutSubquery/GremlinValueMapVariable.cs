using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinValueMapVariable : GremlinMapTableVariable
    {
        public bool IsIncludeTokens { get; set; }
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinValueMapVariable(GremlinVariable inputVariable, bool isIncludeTokens, List<string>  propertyKeys)
        {
            GremlinVariableType inputVariableType = inputVariable.GetVariableType();
            if (!(inputVariableType <= GremlinVariableType.Unknown))
            {
                throw new SyntaxErrorException("The inputVariable of valueMap() can not be " + inputVariableType);
            }

            this.InputVariable = inputVariable;
            this.IsIncludeTokens = isIncludeTokens;
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
            parameters.Add(SqlUtil.GetValueExpr(this.IsIncludeTokens ? 1: -1));
            parameters.AddRange(this.PropertyKeys.Select(SqlUtil.GetValueExpr));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.ValueMap, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
