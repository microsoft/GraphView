using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinWrapVariable: GremlinContextVariable
    {
        public GremlinWrapVariable(GremlinVariable variable): base(variable)
        {
        }

    }
}
