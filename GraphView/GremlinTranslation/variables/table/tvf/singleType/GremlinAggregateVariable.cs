using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAggregateVariable : GremlinTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinVariable AggregateVariable { get; set; }

        public GremlinAggregateVariable(GremlinVariable aggregateVariable, string sideEffectKey)
        {
            AggregateVariable = aggregateVariable;
            SideEffectKey = sideEffectKey;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(AggregateVariable.ToCompose1());
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Aggregate, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
