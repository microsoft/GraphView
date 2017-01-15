using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGhostVariable: GremlinVariable
    {
        public GremlinVariable RealVariable { get; set; }
        public GremlinVariable AttachedVariable { get; set; }

        public GremlinGhostVariable(GremlinVariable realVariable, GremlinVariable attachedVariable)
        {
            RealVariable = realVariable;
            AttachedVariable = attachedVariable;
        }
    }
}
