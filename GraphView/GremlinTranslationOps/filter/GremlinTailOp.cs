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
            //GremlinDerivedVariable newDerivedVariable = new GremlinDerivedVariable(inputContext.ToSelectQueryBlock());
            //newContext.AddNewVariable(newDerivedVariable);
            //newContext.SetDefaultProjection(newDerivedVariable);
            //newContext.SetCurrVariable(newDerivedVariable);

            if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                var existInPath = inputContext.Paths.Find(p => p.Item2 == inputContext.CurrVariable);
                existInPath.Item3.Low = 0 - Limit;
                existInPath.Item3.High = 0;
            }
            else
            {
                inputContext.CurrVariable.Low = 0 - Limit;
                inputContext.CurrVariable.High = 0;
            }

            return inputContext;
        }

    }
}
