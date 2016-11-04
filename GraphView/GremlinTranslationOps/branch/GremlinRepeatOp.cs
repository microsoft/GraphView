using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.branch
{
    internal class GremlinRepeatOp: GremlinTranslationOperator
    {
        GremlinTranslationOperator ParamOp;

        public GremlinRepeatOp(GremlinTranslationOperator paramOp)
        {
            ParamOp = paramOp;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            return inputContext;
        }

    }
}
