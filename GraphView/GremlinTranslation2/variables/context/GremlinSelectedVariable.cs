using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView {
    internal class GremlinSelectedVariable: GremlinVariable
    {
        public bool IsFromSelect { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }
        public string SelectKey { get; set; }
        public List<string> UsedProperties { get; set; }
        public GremlinVariable RealVariable { get; set; }

        public GremlinVariable GetRealVariable()
        {
            if (RealVariable is GremlinSelectedVariable)
            {
                return (RealVariable as GremlinSelectedVariable).GetRealVariable();
            }
            return RealVariable;
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            return RealVariable.DefaultVariableProperty();
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return RealVariable.DefaultProjection();
        }
    }
}
