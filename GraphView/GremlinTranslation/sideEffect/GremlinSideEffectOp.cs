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

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            SideEffectTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext sideEffectContext = SideEffectTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.SideEffect(inputContext, sideEffectContext);

            return inputContext;

        }
    }
}
