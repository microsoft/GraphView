using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSideEffectOp: GremlinTranslationOperator
    {
        public GraphTraversal SideEffectTraversal { get; set; }

        public GremlinSideEffectOp(GraphTraversal sideEffectTraversal)
        {
            SideEffectTraversal = sideEffectTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of sideEffect()-step can't be null.");
            }

            SideEffectTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext sideEffectContext = SideEffectTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.SideEffect(inputContext, sideEffectContext);

            return inputContext;

        }
    }
}
