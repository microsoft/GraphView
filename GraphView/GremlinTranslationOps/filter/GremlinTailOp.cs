using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinTailOp: GremlinTranslationOperator
    {
        public long Limit;

        public GremlinTailOp()
        {
            Limit = 1;
        }

        public GremlinTailOp(long limit)
        {
            Limit = limit;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("tail", Limit));

            GremlinToSqlContext newContext = new GremlinToSqlContext();
            GremlinDerivedVariable newDerivedVariable = new GremlinDerivedVariable(inputContext.ToSqlQuery());
            newContext.AddNewVariable(newDerivedVariable);
            newContext.SetDefaultProjection(newDerivedVariable);
            newContext.SetCurrVariable(newDerivedVariable);

            return inputContext;
        }

    }
}
