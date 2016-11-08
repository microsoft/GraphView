using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinAsOp: GremlinTranslationOperator
    {
        public List<string> StepLabels;
        public GremlinAsOp(params string[] stepLabels) {

            StepLabels = new List<string>();
            foreach (var label in stepLabels)
            {
                StepLabels.Add(label);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            foreach (var stepLabel in StepLabels)
            {
                inputContext.AliasToGremlinVariable.Add(stepLabel, inputContext.CurrVariableList.Copy());
            }

            return inputContext;
        }
    }
}
