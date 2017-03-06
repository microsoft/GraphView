using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 LocalTraversal { get; set; }

        public GremlinLocalOp(GraphTraversal2 localTraversal)
        {
            LocalTraversal = localTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            LocalTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext localContext = LocalTraversal.GetEndOp().GetContext();

            inputContext.PivotVariable.Local(inputContext, localContext);

            return inputContext;
        }
    }
}
