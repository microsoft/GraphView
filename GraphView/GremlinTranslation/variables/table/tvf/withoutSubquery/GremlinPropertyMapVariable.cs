using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertyMapVariable : GremlinTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinContextVariable InputVariable { get; set; }

        public GremlinPropertyMapVariable(GremlinVariable inputVariable, List<string> propertyKeys) : base(GremlinVariableType.Table)
        {
            InputVariable = new GremlinContextVariable(inputVariable);
            PropertyKeys = propertyKeys;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(InputVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            foreach (var propertyKey in PropertyKeys)
            {
                parameters.Add(SqlUtil.GetValueExpr(propertyKey));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.PropertyMap, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
