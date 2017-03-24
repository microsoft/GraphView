using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVariable: GremlinVariable
    {
        public GremlinVariable RealVariable { get; set; }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override string GetVariableName()
        {
            return RealVariable.GetVariableName();
        }

        public GremlinContextVariable(GremlinVariable contextVariable)
        {
            RealVariable = contextVariable;
        }

        internal override GremlinVariableProperty GetVariableProperty(string property)
        {
            Populate(property);
            return RealVariable.GetVariableProperty(property);
        }

        internal override void Populate(string property)
        {
            base.Populate(property);
            RealVariable.Populate(property);
        }

        internal override void PopulateStepProperty(string property)
        {
            base.PopulateStepProperty(property);
            RealVariable.PopulateStepProperty(property);
        }
    }
}
