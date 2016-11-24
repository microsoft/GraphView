using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinNotOp: GremlinTranslationOperator
    {
        public GraphTraversal2 NotTraversal;

        public GremlinNotOp(GraphTraversal2 notTraversal)
        {
            NotTraversal = notTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            return inputContext;
        }
    }
}
