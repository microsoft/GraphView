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
            //WScalarExpression valueExpr = GremlinUtil.GetValueExpression(Limit.ToString());
            //inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("tail", valueExpr));

            //GremlinToSqlContext newContext = new GremlinToSqlContext();
            //GremlinDerivedVariable newDerivedVariable = new GremlinDerivedVariable(inputContext.ToSqlQuery());
            //newContext.AddNewVariable(newDerivedVariable);
            //newContext.SetDefaultProjection(newDerivedVariable);
            //newContext.SetCurrVariable(newDerivedVariable);

            GremlinRangeVariable newVar = new GremlinRangeVariable(-1, Limit);
            inputContext.AddNewVariable(newVar);
            // TODO
            // Projection ??
            inputContext.SetCurrVariable(newVar);

            return inputContext;
        }

    }
}
