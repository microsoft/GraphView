using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSideEffectOp: GremlinTranslationOperator
    {
        public GraphTraversal2 SideEffectTraversal { get; set; }

        public GremlinSideEffectOp(GraphTraversal2 sideEffectTraversal)
        {
            SideEffectTraversal = sideEffectTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedContextFromParent(SideEffectTraversal, inputContext);
            GremlinToSqlContext sideEffectContext = SideEffectTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.SideEffect(inputContext, sideEffectContext);

            return inputContext;

        }
    }
}
