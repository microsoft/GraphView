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

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            //WScalarExpression valueExpr = GremlinUtil.GetValueExpression(Limit.ToString());
            //inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("limit", valueExpr));

            //GremlinToSqlContext newContext = new GremlinToSqlContext();
            //GremlinDerivedVariable newDerivedVariable = new GremlinDerivedVariable(inputContext.ToSelectQueryBlock());
            //newContext.AddNewVariable(newDerivedVariable);
            //newContext.SetDefaultProjection(newDerivedVariable);
            //newContext.SetCurrVariable(newDerivedVariable);

            if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                var sinkNode = inputContext.GetSinkNode(inputContext.CurrVariable);
                sinkNode.Low = 0;
                sinkNode.High = Limit;
            }
            else
            {
                inputContext.CurrVariable.Low = 0;
                inputContext.CurrVariable.High = Limit;
            }

            return inputContext;
        }
    }
}
