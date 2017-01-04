using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView {

    internal abstract class GremlinEdgeVariable2 : GremlinTableVariable
    {
        public WEdgeType EdgeType { get; set; }

        // SourceVariable is used for saving the variable which the edge come from
        // It's used for otherV step
        // For example: g.V().outE().otherV()
        // g.V() generate n_0
        // then we have a match clause n_0-[edge as e_0]->n_1
        // we user calls otherV(), we will know the n_0 is the source vertex, and then n_1 will be the otherV
        public GremlinVariable2 SourceVariable { get; set; }
    }
}
