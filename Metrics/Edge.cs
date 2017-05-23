using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Metrics
{
    class Edge
    {
        public int V, U;
        public double W;

        public Edge(int V, int U, double W)
        {
            this.V = V;
            this.U = U;
            this.W = W;
        }
    }
}
