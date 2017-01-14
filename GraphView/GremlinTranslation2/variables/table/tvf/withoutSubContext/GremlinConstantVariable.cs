using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinConstantVariable: GremlinScalarTableVariable
    {
        public object Value { get; set; }

        public GremlinConstantVariable(object value)
        {
            Value = value;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (Value is List<object>)
            {
                foreach (var value in Value as List<object>)
                {
                    parameters.Add(SqlUtil.GetValueExpr(value));
                }
            }
            else
            {
                parameters.Add(SqlUtil.GetValueExpr(Value));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Constant, parameters, this, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
