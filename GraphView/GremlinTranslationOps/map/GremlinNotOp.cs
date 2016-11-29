using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinNotOp: GremlinTranslationOperator
    {
        public GraphTraversal2 NotTraversal;

        public GremlinNotOp(GraphTraversal2 notTraversal)
        {
            NotTraversal = notTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WQueryDerivedTable queryDerivedTable = new WQueryDerivedTable()
            {
                QueryExpr = NotTraversal.GetEndOp().GetContext().ToSqlQuery() as WSelectQueryBlock
            };
            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(queryDerivedTable, "not");
            inputContext.AddNewVariable(newVariable, Labels); //??Labels?

            WBooleanComparisonExpression booleanComparisonExpr = new WBooleanComparisonExpression()
            {
                ComparisonType = BooleanComparisonType.NotEqualToExclamation,
                FirstExpr = GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.VariableName, "id"),
                SecondExpr = GremlinUtil.GetColumnReferenceExpression(newVariable.VariableName, "id")
            };
            inputContext.AddPredicate(booleanComparisonExpr);

            return inputContext;
        }
    }
}
