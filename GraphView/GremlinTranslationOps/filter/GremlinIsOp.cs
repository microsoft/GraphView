using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinIsOp: GremlinTranslationOperator
    {
        public object Value;
        public Predicate Predicate;

        public GremlinIsOp(object value)
        {
            Value = value;
        }

        public GremlinIsOp(Predicate predicate)
        {
            Predicate = predicate;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            return inputContext;
        }
    }
}
