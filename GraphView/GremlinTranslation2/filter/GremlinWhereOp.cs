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
        }
        public GremlinWhereOp(string startKey, Predicate predicate)
        {
            StartKey = startKey;
            Predicate = predicate;
        }

        public GremlinWhereOp(GraphTraversal2 whereTraversal)
        {
            WhereTraversal = whereTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //if (Predicate != null && Predicate.IsAliasValue)
            //{

            //}

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
                inputContext.PivotVariable.Where(inputContext, WhereTraversal);
            }
            return inputContext;
        }
    }
}
