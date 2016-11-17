using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinMatchOp: GremlinTranslationOperator
    {
        public IList<GremlinTranslationOperator> ConjunctiveOperators;

        public GremlinMatchOp(params GraphTraversal2[] matchTraversals)
        {
            ConjunctiveOperators = new List<GremlinTranslationOperator>();
            foreach (var traversal in matchTraversals)
            {
                ConjunctiveOperators.Add(traversal.LastGremlinTranslationOp);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<GremlinToSqlContext> matchContexts = new List<GremlinToSqlContext>();
            foreach (GremlinTranslationOperator matchOp in ConjunctiveOperators)
            {
                var rootOp = matchOp;
                while (rootOp.InputOperator != null)
                {
                    rootOp = rootOp.InputOperator;
                }

                if (rootOp.GetType() == typeof(GremlinParentContextOp))
                {
                    GremlinParentContextOp rootAsContext = rootOp as GremlinParentContextOp;
                    rootAsContext.InheritedVariable = inputContext.CurrVariable;
                }

                matchContexts.Add(matchOp.GetContext());
            }

            return inputContext;
        }
    }
}
