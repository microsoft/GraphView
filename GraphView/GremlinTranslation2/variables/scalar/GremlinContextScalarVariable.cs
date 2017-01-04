using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextScalarVariable : GremlinContextVariable
    {
        public GremlinContextScalarVariable(GremlinVariable2 contextVariable) : base(contextVariable) { }

        internal override GremlinVariableType GetVariableType()
        {
            return ContextVariable.GetVariableType();
        }
    }
}
