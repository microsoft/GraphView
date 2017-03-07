using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSumLocalVariable : GremlinScalarTableVariable
    {
        public GremlinVariable InputVariable { get; set; }

        public GremlinSumLocalVariable(GremlinVariable inputVariable)
        {
            InputVariable = inputVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SumLocal, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
