using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSubgraphOp: GremlinTranslationOperator
    {
        public string SideEffectKey { get; set; }

        public GremlinSubgraphOp(string sideEffectKey)
        {
            SideEffectKey = sideEffectKey;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of subgraph()-step can't be null.");
            }

            GraphTraversal traversalAux = GraphTraversal.__();
                traversalAux.GetStartOp().InheritedVariableFromParent(inputContext);

            inputContext.PivotVariable.Subgraph(inputContext, SideEffectKey, traversalAux.GetEndOp().GetContext());

            return inputContext;
        }
    }
}
