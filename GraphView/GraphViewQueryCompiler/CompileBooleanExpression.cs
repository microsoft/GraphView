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
            ScalarFunction f1 = FirstExpr.CompileToFunction(context, dbConnection);
            ScalarFunction f2 = SecondExpr.CompileToFunction(context, dbConnection);

            return new ComparisonFunction(f1, f2, ComparisonType);
        }
    }

    public partial class WInPredicate
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            ScalarFunction lhsFunction;
            if (this.Expression != null) {
                lhsFunction = this.Expression.CompileToFunction(context, dbConnection);
            }
            else if (this.Subquery != null) {
                lhsFunction = this.Subquery.CompileToFunction(context, dbConnection);
            }
            else {
                throw new QueryCompilationException("Expression and Subquery can't all be null in a WInPredicate.");
            }

            List<ScalarFunction> values = this.Values.Select(value => value.CompileToFunction(context, dbConnection)).ToList();

            return new InFunction(lhsFunction, values, this.NotDefined);
        }
    }

    public partial class WBooleanNotExpression
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            return new BooleanNotFunction(Expression.CompileToFunction(context, dbConnection));
        }
    }

    public partial class WBooleanParenthesisExpression
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            return Expression.CompileToFunction(context, dbConnection);
        }
    }

    public partial class WExistsPredicate
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            QueryCompilationContext subContext = new QueryCompilationContext(context);
            GraphViewExecutionOperator subQueryOp = Subquery.SubQueryExpr.Compile(subContext, dbConnection);
            ExistsFunction existsFunc = new ExistsFunction(subQueryOp, subContext.OuterContextOp);

            return existsFunc;
        }
    }
}
