using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinConstantVariable: GremlinTableVariable
    {
        public object Value { get; set; }

        public GremlinConstantVariable(object value)
        {
            Value = value;
            VariableName = GenerateTableAlias();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "_value");
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(SqlUtil.GetValueExpr(Value));
            var secondTableRef = SqlUtil.GetFunctionTableReference("constant", PropertyKeys, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
