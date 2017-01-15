using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBranchVariable: GremlinVariable
    {
        public List<List<GremlinVariable>> BrachVariableList { get; set; }

        public GremlinBranchVariable()
        {
            BrachVariableList = new List<List<GremlinVariable>>();
        }
    }
}
