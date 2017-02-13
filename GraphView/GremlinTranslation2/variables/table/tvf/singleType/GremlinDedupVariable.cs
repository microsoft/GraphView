using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDedupVariable : GremlinTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public List<string> DedupLabels { get; set; }

        public GremlinDedupVariable(GremlinVariable inputVariable, List<string> dedupLabels)
        {
            InputVariable = inputVariable;
            DedupLabels = new List<string>(dedupLabels);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(InputVariable.DefaultVariableProperty().ToScalarExpression());
            foreach (var dedupLabel in DedupLabels)
            {
                //TODO:
                throw new NotImplementedException();
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Dedup, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
