using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextTableVariable: GremlinTableVariable
    {
        public GremlinVariable2 ContextVariable;

        public GremlinContextTableVariable(GremlinVariable2 contextVariable)
        {
            ContextVariable = contextVariable;
        }
    }
}
