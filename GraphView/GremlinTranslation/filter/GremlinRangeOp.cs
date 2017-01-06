using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinRangeOp: GremlinTranslationOperator
    {
        public int Low { get; set; }
        public int High { get; set; }
        public GremlinRangeOp(int low, int high)
        {
            Low = low;
            High = high;
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            //WScalarExpression lowExpr = GremlinUtil.GetValueExpr(Low.ToString());
            //WScalarExpression highExpr = GremlinUtil.GetValueExpr(High.ToString());
            //inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("range", lowExpr, highExpr));
            
            //GremlinToSqlContext newContext = new GremlinToSqlContext();
            //GremlinDerivedVariable newDerivedVariable = new GremlinDerivedVariable(inputContext.ToSelectQueryBlock());
            //newContext.AddNewVariable(newDerivedVariable);
            //newContext.SetDefaultProjection(newDerivedVariable);
            //newContext.SetCurrVariable(newDerivedVariable);

            if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                var sinkNode = inputContext.GetSinkNode(inputContext.CurrVariable);
                sinkNode.Low = Low;
                sinkNode.High = High;
            }
            else
            {
                inputContext.CurrVariable.Low = Low;
                inputContext.CurrVariable.High = High;
            }

            return inputContext;
        }
    }
}
