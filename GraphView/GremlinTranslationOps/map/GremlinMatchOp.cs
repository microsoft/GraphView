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

        public GremlinMatchOp()
        {

        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            return inputContext;
        }
    }
}
