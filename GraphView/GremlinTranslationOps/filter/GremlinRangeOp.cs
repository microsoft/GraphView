using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinRangeOp: GremlinTranslationOperator
    {
        public long Low;
        public long High;
        public GremlinRangeOp(long low, long high)
        {
            Low = low;
            High = high;
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("range", Low, High));
            
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            GremlinDerivedVariable newDerivedVariable = new GremlinDerivedVariable(inputContext.ToSqlQuery());
            newContext.AddNewVariable(newDerivedVariable);
            newContext.SetDefaultProjection(newDerivedVariable);
            newContext.SetCurrVariable(newDerivedVariable);

            //TODO: inherit some variable?

            return newContext;
        }
    }
}
