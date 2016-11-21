using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinCoalesceStep: GremlinTranslationOperator
    {
        public List<GraphTraversal2> CoalesceTraversals;
        public GremlinCoalesceStep(params GraphTraversal2[] coalesceTraversals)
        {
            CoalesceTraversals = new List<GraphTraversal2>();
            foreach (var coalesceTraversal in coalesceTraversals)
            {
                CoalesceTraversals.Add(coalesceTraversal);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inpuContext = GetInputContext();


            return inpuContext;
        }
    }
}
