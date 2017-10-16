using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalOp: GremlinTranslationOperator
    {
        public GraphTraversal LocalTraversal { get; set; }

        public GremlinLocalOp(GraphTraversal localTraversal)
        {
            LocalTraversal = localTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of local()-step can't be null.");
            }

            LocalTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext localContext = LocalTraversal.GetEndOp().GetContext();

            inputContext.PivotVariable.Local(inputContext, localContext);

            return inputContext;
        }
    }
}
