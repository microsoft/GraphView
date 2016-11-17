using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinCoinOp: GremlinTranslationOperator
    {
        public double Probability;

        public GremlinCoinOp(double probability)
        {
            Probability = probability;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WColumnReferenceExpression columnRefExpr =
                GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.VariableName);
            WValueExpression valueExpr = GremlinUtil.GetValueExpression(Probability);
            WFunctionCall functionCall = GremlinUtil.GetFunctionCall("coin", columnRefExpr, valueExpr);

            WColumnReferenceExpression trueExpr = GremlinUtil.GetColumnReferenceExpression("true");

            WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(functionCall, trueExpr, BooleanComparisonType.Equals);

            inputContext.AddPredicate(booleanExpr);

            return inputContext;
        }
    }
}
