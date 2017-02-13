using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinTailOp: GremlinTranslationOperator
    {
        public long Limit { get; set; }

        public GremlinTailOp()
        {
            Limit = 1;
        }

        public GremlinTailOp(long limit)
        {
            Limit = limit;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
        }

    }
}
