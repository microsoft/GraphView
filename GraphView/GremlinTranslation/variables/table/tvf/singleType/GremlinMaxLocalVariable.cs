using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMaxLocalVariable : GremlinScalarTableVariable
    {
        public GremlinVariable InputVariable { get; set; }

        public GremlinMaxLocalVariable(GremlinVariable inputVariable)
        {
            InputVariable = inputVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.MaxLocal, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
