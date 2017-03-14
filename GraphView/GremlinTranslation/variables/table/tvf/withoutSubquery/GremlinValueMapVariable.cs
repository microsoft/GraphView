using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinValueMapVariable : GremlinTableVariable
    {
        public bool IsIncludeTokens { get; set; }
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinValueMapVariable(GremlinVariable inputVariable, bool isIncludeTokens, List<string>  propertyKeys) : base(GremlinVariableType.Table)
        {
            InputVariable = inputVariable;
            IsIncludeTokens = isIncludeTokens;
            PropertyKeys = propertyKeys;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(IsIncludeTokens ? 1: -1));
            foreach (var propertyKey in PropertyKeys)
            {
                parameters.Add(SqlUtil.GetValueExpr(propertyKey));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.ValueMap, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
