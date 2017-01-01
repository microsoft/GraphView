using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapOp: GremlinTranslationOperator
    {
        internal GraphTraversal2 FlatMapTraversal { get; set; }

        public GremlinFlatMapOp(GraphTraversal2 flatMapTraversal)
        {
            FlatMapTraversal = flatMapTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }
    }
}
