using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSubgraphVariable : GremlinScalarTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinToSqlContext DummyContext { get; set; }

        public GremlinSubgraphVariable(GremlinToSqlContext dummyContext, string sideEffectKey)
        {
            this.SideEffectKey = sideEffectKey;
            this.DummyContext = dummyContext;
            this.Labels.Add(sideEffectKey);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            WSelectQueryBlock selectQueryBlock = this.DummyContext.ToSelectQueryBlock(false);
            parameters.Add(SqlUtil.GetScalarSubquery(selectQueryBlock));
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Subgraph, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
