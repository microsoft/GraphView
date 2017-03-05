using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinWhereOp: GremlinTranslationOperator
    {
        public Predicate Predicate { get; set; }
        public string StartKey { get; set; }
        public GraphTraversal2 WhereTraversal { get; set; }

        public GremlinWhereOp(Predicate predicate)
        {
            Predicate = predicate;
            Predicate.IsTag = true;
        }
        public GremlinWhereOp(string startKey, Predicate predicate)
        {
            StartKey = startKey;
            Predicate = predicate;
            Predicate.IsTag = true;
        }

        public GremlinWhereOp(GraphTraversal2 whereTraversal)
        {
            WhereTraversal = whereTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (WhereTraversal == null && StartKey == null)
            {
                //where(Predicate)
                //use Predicates
                inputContext.PivotVariable.Where(inputContext, Predicate);
            }
            else if (WhereTraversal == null && StartKey != null)
            {
                //where(StartKey, Predicate)
                throw new NotImplementedException();
            }
            else
            {
                //where(whereTraversal)
                //use Exist
                WhereTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                GremlinToSqlContext whereContext = WhereTraversal.GetEndOp().GetContext();
                inputContext.PivotVariable.Where(inputContext, whereContext);
            }
            return inputContext;
        }
    }
}
