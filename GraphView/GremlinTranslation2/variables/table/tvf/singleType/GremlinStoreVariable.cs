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

        public GremlinStoreVariable(string sideEffectKey)
        {
            SideEffectKey = sideEffectKey;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            //parameters.Add(SideEffectKey);
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Store, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
