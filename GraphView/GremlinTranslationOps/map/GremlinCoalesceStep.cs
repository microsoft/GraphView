using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinCoalesceStep: GremlinTranslationOperator
    {
        public List<GraphTraversal2> CoalesceTraversals;
        public GremlinCoalesceStep(params GraphTraversal2[] coalesceTraversals)
        {
            CoalesceTraversals = new List<GraphTraversal2>();
            foreach (var coalesceTraversal in coalesceTraversals)
            {
                CoalesceTraversals.Add(coalesceTraversal);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WCoalesce2 coalesceExpr = new WCoalesce2() { CoalesceQuery = new List<WSqlStatement>()};
            
            foreach (var coalesceTraversal in CoalesceTraversals)
            {
                GremlinUtil.InheritedVariableFromParent(coalesceTraversal, inputContext);
                coalesceExpr.CoalesceQuery.Add(coalesceTraversal.GetEndOp().GetContext().ToSelectQueryBlock());
            }

            GremlinCoalesceVariable newVariable = new GremlinCoalesceVariable(coalesceExpr);
            inputContext.AddNewVariable(newVariable, Labels);

            return inputContext;
        }
    }
}
