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
            //WScalarExpression lowExpr = GremlinUtil.GetValueExpression(Low.ToString());
            //WScalarExpression highExpr = GremlinUtil.GetValueExpression(High.ToString());
            //inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("range", lowExpr, highExpr));
            
            //GremlinToSqlContext newContext = new GremlinToSqlContext();
            //GremlinDerivedVariable newDerivedVariable = new GremlinDerivedVariable(inputContext.ToSqlQuery());
            //newContext.AddNewVariable(newDerivedVariable);
            //newContext.SetDefaultProjection(newDerivedVariable);
            //newContext.SetCurrVariable(newDerivedVariable);

            //TODO: inherit some variable?


            GremlinRangeVariable newVar = new GremlinRangeVariable(Low, High);
            inputContext.AddNewVariable(newVar);
            // TODO
            // Projection ??
            inputContext.SetCurrVariable(newVar);

            return inputContext;
        }
    }
}
