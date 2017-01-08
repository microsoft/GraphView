using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSideEffectVariable: GremlinTableVariable
    {
        public GremlinToSqlContext SideEffectContext { get; set; }

        public GremlinSideEffectVariable(GremlinToSqlContext sideEffectContext)
        {
            SideEffectContext = sideEffectContext;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(SideEffectContext.ToSelectQueryBlock()));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SideEffect, parameters, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
