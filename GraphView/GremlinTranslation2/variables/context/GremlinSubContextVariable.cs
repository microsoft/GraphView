using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSubContextVariable: GremlinVariable
    {
        public GremlinVariable SubContextVariable { get; set; }

        public GremlinSubContextVariable(GremlinVariable subContextVariable)
        {
            SubContextVariable = subContextVariable;
        }

    }
}
