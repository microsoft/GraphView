using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace GraphView
{
    internal class GremlinMinLocalVariable : GremlinScalarTableVariable
    {
        public GremlinVariable InputVariable { get; set; }

        public GremlinMinLocalVariable(GremlinVariable inputVariable)
        {
            GremlinVariableType inputVariableType = inputVariable.GetVariableType();
            if (!(inputVariableType == GremlinVariableType.List || GremlinVariableType.NULL <= inputVariableType && inputVariableType <= GremlinVariableType.Unknown))
            {
                throw new SyntaxErrorException("The inputVariable of min(local) can not be " + inputVariableType);
            }

            this.InputVariable = inputVariable;
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
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.MinLocal, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
