using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinAsStep: GremlinTranslationOperator
    {
        public string StepLabel;
        public List<string> StepLabels;
        public GremlinAsStep(string stepLabel, params string[] stepLabels) {
            StepLabel = stepLabel;

            StepLabels = new List<string>();
            foreach (var label in stepLabels)
            {
                StepLabels.Add(label);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            return inputContext;
        }
    }
}
