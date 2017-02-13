using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatSelectedVariable: GremlinSelectedVariable
    {
        public GremlinVariable AttachedRepeatVariable { get; set; }
        public string ColumnName { get; set; }

        public GremlinRepeatSelectedVariable(GremlinVariable attachedRepeatVarible, GremlinVariable realVariable, string columnName)
        {
            AttachedRepeatVariable = attachedRepeatVarible;
            RealVariable = realVariable;
            ColumnName = columnName;
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            return new GremlinVariableProperty(AttachedRepeatVariable, ColumnName);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override void BottomUpPopulate(GremlinVariable terminateVariable, string property, string columnName)
        {
            RealVariable.BottomUpPopulate(terminateVariable, property, columnName);
        }
    }
}
