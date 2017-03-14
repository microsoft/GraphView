using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSampleVariable : GremlinTableVariable
    {
        public GremlinKeyword.Scope Scope { get; set; }
        public int AmountToSample { get; set; }
        public GremlinToSqlContext ProbabilityContext { get; set; }

        public GremlinSampleVariable(GremlinKeyword.Scope scope, int amountToSample, GremlinToSqlContext probabilityContext)
            : base(GremlinVariableType.Table)
        {
            Scope = scope;
            AmountToSample = amountToSample;
            ProbabilityContext = probabilityContext;
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return ProbabilityContext.FetchVarsFromCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(AmountToSample));
            if (ProbabilityContext != null)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(ProbabilityContext.ToSelectQueryBlock()));
            }
            var tableRef = Scope == GremlinKeyword.Scope.global
                ? SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SampleGlobal, parameters, GetVariableName())
                : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SampleLocal, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
