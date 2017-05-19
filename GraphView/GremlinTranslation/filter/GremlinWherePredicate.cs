using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinWherePredicateOp: GremlinTranslationOperator
    {
        public Predicate Predicate { get; set; }
        public string StartKey { get; set; }
        public List<GraphTraversal2> ByTraversals { get; set; }

        public GremlinWherePredicateOp(Predicate predicate)
        {
            Predicate = predicate;
            Predicate.IsTag = true;
            ByTraversals = new List<GraphTraversal2>();
        }

        public GremlinWherePredicateOp(string startKey, Predicate predicate)
        {
            StartKey = startKey;
            Predicate = predicate;
            Predicate.IsTag = true;
            ByTraversals = new List<GraphTraversal2>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (ByTraversals.Count == 0)
            {
                ByTraversals.Add(GraphTraversal2.__());
            }

            if (StartKey == null)
            {
                //where(Predicate)
                //use Predicates
                inputContext.PivotVariable.Where(inputContext, Predicate, new TraversalRing(ByTraversals));
            }
            else
            {
                //where(StartKey, Predicate)
                inputContext.PivotVariable.Where(inputContext, StartKey, Predicate, new TraversalRing(ByTraversals));
            }

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByTraversals.Add(traversal);
        }
    }
}
