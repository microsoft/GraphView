using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVariable: GremlinSelectedVariable
    {
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
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            RealVariable.Populate(property);
        }

        internal override void Select(GremlinToSqlContext currentContext, List<string> Labels)
        {
            RealVariable.Select(currentContext, Labels);
        }

        internal override void Select(GremlinToSqlContext currentContext, string selectKey)
        {
            RealVariable.Select(currentContext, selectKey);
        }
    }
}
