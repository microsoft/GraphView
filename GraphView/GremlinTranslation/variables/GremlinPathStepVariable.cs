using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathStepVariable: GremlinVariable
    {
        public List<GremlinVariable> StepVariable { get; set; }
        public GremlinVariable AttachedVariable { get; set; }

        public GremlinPathStepVariable(GremlinVariable stepVariable, GremlinVariable attachedVariable = null)
        {
            StepVariable = new List<GremlinVariable> { stepVariable};
            AttachedVariable = attachedVariable;
        }

        public GremlinPathStepVariable(List<GremlinVariable> stepVariables, GremlinVariable attachedVariable = null)
        {
            StepVariable = stepVariables;
            AttachedVariable = attachedVariable;
        }

        internal override void Populate(string property)
        {
            foreach (var branchStep in StepVariable)
            {
                branchStep.Populate(property);
            }
            base.Populate(property);
        }
    }
}
