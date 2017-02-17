using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatSelectedVariable: GremlinSelectedVariable
    {
        public GremlinVariable AttachedVariable { get; set; }

        public GremlinRepeatSelectedVariable(GremlinVariable attachedRepeatVarible, GremlinVariable realVariable, string selectKey)
        {
            RealVariable = realVariable;
            AttachedVariable = attachedRepeatVarible;
            SelectKey = selectKey;
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            return new GremlinVariableProperty(AttachedVariable, SelectKey);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            RealVariable.Populate(property);
        }

        internal override void BottomUpPopulate(GremlinVariable terminateVariable, string property, string columnName)
        {
            RealVariable.BottomUpPopulate(terminateVariable, property, columnName);
        }
    }
}
