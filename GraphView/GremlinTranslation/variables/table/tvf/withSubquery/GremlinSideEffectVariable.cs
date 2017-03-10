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
            : base(GremlinVariableType.Table)
        {
            SideEffectContext = sideEffectContext;
            SideEffectContext.HomeVariable = this;
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            //SideEffect step should be regarded as one step, so we can't populate the tagged variable of coalesceContextList 
            return new List<GremlinVariable>();
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return SideEffectContext.FetchVarsFromCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(SideEffectContext.ToSelectQueryBlock()));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SideEffect, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
