using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinWhereOp: GremlinTranslationOperator
    {
        public Predicate Predicate;
        public string StartKey;
        public GremlinTranslationOperator ParamOp;

        public GremlinWhereOp(Predicate predicate)
        {
            Predicate = predicate;
        }
        public GremlinWhereOp(string startKey, Predicate predicate)
        {
            StartKey = startKey;
            Predicate = predicate;
        }

        public GremlinWhereOp(GremlinTranslationOperator paramOp)
        {
            ParamOp = paramOp;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            WBooleanExpression andExpression = null;

            if (ParamOp == null && StartKey == null)
            {
                //where(Predicate)

            }
            else if (ParamOp == null && StartKey != null)
            {
                //where(StartKey, Predicate)
            }
            else
            {
                //where(whereTraversal)
            }

            return inputContext;
        }
    }
}
