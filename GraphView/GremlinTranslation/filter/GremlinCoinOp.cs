using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinCoinOp: GremlinTranslationOperator
    {
        public double Probability { get; set; }

        public GremlinCoinOp(double probability)
        {
            Probability = probability;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

                
            List<WScalarExpression> parameterList = new List<WScalarExpression>()
            {
                //GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.VariableName),
                GremlinUtil.GetValueExpression(Probability)
            };
            WFunctionCall functionCall = GremlinUtil.GetFunctionCall("coin", parameterList);

            WColumnReferenceExpression trueExpr = GremlinUtil.GetColumnReferenceExpression("true");

            WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(functionCall, trueExpr, BooleanComparisonType.Equals);

            inputContext.AddPredicate(booleanExpr);

            return inputContext;
        }
    }
}
