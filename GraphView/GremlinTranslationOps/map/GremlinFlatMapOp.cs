using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinFlatMapOp: GremlinTranslationOperator
    {
        internal GraphTraversal2 FlatMapTraversal;

        public GremlinFlatMapOp(GraphTraversal2 flatMapTraversal)
        {
            FlatMapTraversal = flatMapTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedContextFromParent(FlatMapTraversal, inputContext);

            inputContext = FlatMapTraversal.GetEndOp().GetContext();
            inputContext.SetLabelsToCurrentVariable(Labels);
            return inputContext;
        }
    }
}
