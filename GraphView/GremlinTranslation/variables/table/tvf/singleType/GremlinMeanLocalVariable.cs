using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMeanLocalVariable : GremlinScalarTableVariable
    {
        public GremlinVariable InputVariable { get; set; }

        public GremlinMeanLocalVariable(GremlinVariable inputVariable)
        {
            InputVariable = inputVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.MeanLocal, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}