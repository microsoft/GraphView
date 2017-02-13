using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLimitOp: GremlinTranslationOperator
    {
        public long Limit { get; set; }

        public GremlinLimitOp(long limit)
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
