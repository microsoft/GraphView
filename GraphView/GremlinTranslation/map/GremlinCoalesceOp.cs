using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoalesceOp: GremlinTranslationOperator
    {
        public List<GraphTraversal2> CoalesceTraversals { get; set; }
        public GremlinCoalesceOp(params GraphTraversal2[] coalesceTraversals)
        {
            CoalesceTraversals = new List<GraphTraversal2>();
            foreach (var coalesceTraversal in coalesceTraversals)
            {
                CoalesceTraversals.Add(coalesceTraversal);
            }
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            List<GremlinToSqlContext> coalesceContextList = new List<GremlinToSqlContext>();
            foreach (var traversal in CoalesceTraversals)
            {
                traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                coalesceContextList.Add(traversal.GetEndOp().GetContext());
            }

            inputContext.PivotVariable.Coalesce(inputContext, coalesceContextList);

            return inputContext;
        }
    }
}
