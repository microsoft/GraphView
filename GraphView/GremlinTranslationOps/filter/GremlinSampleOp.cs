using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinSampleOp: GremlinTranslationOperator
    {
        public long AmountToSample;

        public GremlinSampleOp(long amountToSample)
        {
            AmountToSample = amountToSample;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WColumnReferenceExpression columnRefExpr =
                GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.VariableName);
            WValueExpression valueExpr = GremlinUtil.GetValueExpression(AmountToSample);
            WFunctionCall functionCall = GremlinUtil.GetFunctionCall("sample", columnRefExpr, valueExpr);

            WColumnReferenceExpression trueExpr = GremlinUtil.GetColumnReferenceExpression("true");

            WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(functionCall, trueExpr, BooleanComparisonType.Equals);

            inputContext.AddPredicate(booleanExpr);

            //var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("sample", AmountToSample);

            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference, "sample");

            //inputContext.AddNewVariable(newVariable, Labels);
            //inputContext.SetDefaultProjection(newVariable);
            //inputContext.SetCurrVariable(newVariable);

            return inputContext;
        }
    }
}
