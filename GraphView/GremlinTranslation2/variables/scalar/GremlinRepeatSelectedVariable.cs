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
            return GremlinVariableType.Scalar;
        }

        internal override string BottomUpPopulate(string property, GremlinVariable terminateVariable, string alias,
            string columnName = null)
        {
            return RealVariable.BottomUpPopulate(property, terminateVariable, alias, columnName);
        }
    }
}
