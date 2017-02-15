using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinStoreVariable : GremlinTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinVariable StoredVariable { get; set; }

        public GremlinStoreVariable(GremlinVariable storedVariable, string sideEffectKey)
        {
            StoredVariable = storedVariable;
            SideEffectKey = sideEffectKey;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(StoredVariable.ToCompose1());
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Store, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
