using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslation
{
    internal class GremlinSampleOp: GremlinTranslationOperator
    {
        public long AmountToSample { get; set; }

        public GremlinSampleOp(long amountToSample)
        {
            AmountToSample = amountToSample;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<WScalarExpression> parameterList = new List<WScalarExpression>()
            {
                //GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.VariableName),
                GremlinUtil.GetValueExpression(AmountToSample)
            };
            WFunctionCall functionCall = GremlinUtil.GetFunctionCall("sample", parameterList);

            WColumnReferenceExpression trueExpr = GremlinUtil.GetColumnReferenceExpression("true");

            WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(functionCall, trueExpr, BooleanComparisonType.Equals);

            inputContext.AddPredicate(booleanExpr);

            return inputContext;
        }
    }
}
