using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public partial class WBooleanExpression
    {
        internal virtual BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            return null;
        }
    }

    public partial class WBooleanBinaryExpression
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            BooleanFunction bf1 = FirstExpr.CompileToFunction(context, dbConnection);
            BooleanFunction bf2 = SecondExpr.CompileToFunction(context, dbConnection);

            if (BooleanExpressionType == BooleanBinaryExpressionType.And)
            {
                return new BooleanBinaryFunction(bf1, bf2, BooleanBinaryFunctionType.And);
            }
            else
            {
                return new BooleanBinaryFunction(bf1, bf2, BooleanBinaryFunctionType.Or);
            }
        }
    }

    public partial class WBooleanComparisonExpression
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            return null;
        }
    }

    public partial class WExistsPredicate
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            QueryCompilationContext subContext = new QueryCompilationContext(context);
            GraphViewExecutionOperator subQueryOp = Subquery.Compile(subContext, dbConnection);
            ExistsFunction existsFunc = new ExistsFunction(subQueryOp, subContext.outerContextOp);

            return existsFunc;
        }
    }
}
