using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinCoalesceStep: GremlinTranslationOperator
    {
        public List<GremlinTranslationOperator> CoalesceOperators;
        public GremlinCoalesceStep(params GraphTraversal2[] coalesceTraversals)
        {
            CoalesceOperators = new List<GremlinTranslationOperator>();
            foreach (var coalesceTraversal in coalesceTraversals)
            {
                CoalesceOperators.Add(coalesceTraversal.LastGremlinTranslationOp);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WCoalesce2 coalesceExpr = new WCoalesce2() { CoalesceQuery = new List<WSqlStatement>()};
            
            foreach (var coalesceOperator in CoalesceOperators)
            {
                GremlinUtil.InheritedVariableFromParent(coalesceOperator, inputContext);
                coalesceExpr.CoalesceQuery.Add(coalesceOperator.GetContext().ToSqlQuery());
            }

            GremlinCoalesceVariable newVariable = new GremlinCoalesceVariable(coalesceExpr);
            inputContext.AddNewVariable(newVariable);

            return inputContext;
        }
    }
}
