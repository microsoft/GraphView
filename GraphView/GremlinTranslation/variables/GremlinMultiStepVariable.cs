using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMultiStepVariable: GremlinVariable
    {
        public List<GremlinVariable> StepVariable { get; set; }
        public GremlinVariable AttachedVariable { get; set; }

        public GremlinMultiStepVariable(GremlinVariable stepVariable, GremlinVariable attachedVariable = null)
        {
            StepVariable = new List<GremlinVariable> { stepVariable};
            AttachedVariable = attachedVariable;
        }

        public GremlinMultiStepVariable(List<GremlinVariable> stepVariables, GremlinVariable attachedVariable = null)
        {
            StepVariable = stepVariables;
            AttachedVariable = attachedVariable;
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(AttachedVariable, GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var step in StepVariable)
            {
                variableList.AddRange(step.FetchAllVars());
            }
            if (AttachedVariable != null)
                variableList.AddRange(AttachedVariable.FetchAllVars());
            return variableList;
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
