using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinTreeSideEffectVariable : GremlinScalarTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinPathVariable PathVariable { get; set; }

        public GremlinTreeSideEffectVariable(string sideEffectKey, GremlinPathVariable pathVariable)
        {
            SideEffectKey = sideEffectKey;
            PathVariable = pathVariable;
            Labels.Add(sideEffectKey);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return PathVariable.FetchVarsFromCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            parameters.Add(PathVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Tree, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
