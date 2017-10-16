using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoalesceOp: GremlinTranslationOperator
    {
        public List<GraphTraversal> CoalesceTraversals { get; set; }
        public GremlinCoalesceOp(params GraphTraversal[] coalesceTraversals)
        {
            CoalesceTraversals = new List<GraphTraversal>();
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
                throw new TranslationException("The PivotVariable of coalesce()-step can't be null.");
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
