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
        internal virtual BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            return null;
        }

        internal virtual BooleanFunction CompileToBatchFunction(QueryCompilationContext context,
            GraphViewCommand command)
        {
            QueryCompilationContext subContext = new QueryCompilationContext(context);
            subContext.AddField(GremlinKeyword.IndexTableName, command.IndexColumnName, ColumnGraphType.Value, true);
            subContext.InBatchMode = true;

            return this.CompileToFunction(subContext, command);
        }
    }

    public partial class WBooleanBinaryExpression
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            BooleanFunction bf1 = this.FirstExpr.CompileToFunction(context, command);
            BooleanFunction bf2 = this.SecondExpr.CompileToFunction(context, command);

            if (this.BooleanExpressionType == BooleanBinaryExpressionType.And)
            {
                return new BooleanBinaryFunction(bf1, bf2, BooleanBinaryFunctionType.And);
            }
            else
            {
                return new BooleanBinaryFunction(bf1, bf2, BooleanBinaryFunctionType.Or);
            }
        }

        internal override BooleanFunction CompileToBatchFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            BooleanFunction bf1 = this.FirstExpr.CompileToBatchFunction(context, command);
            BooleanFunction bf2 = this.SecondExpr.CompileToBatchFunction(context, command);

            if (this.BooleanExpressionType == BooleanBinaryExpressionType.And)
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
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            ScalarFunction f1 = FirstExpr.CompileToFunction(context, command);
            ScalarFunction f2 = SecondExpr.CompileToFunction(context, command);

            return new ComparisonFunction(f1, f2, ComparisonType);
        }
    }

    public partial class WInPredicate
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            ScalarFunction lhsFunction;
            if (this.Expression != null) {
                lhsFunction = this.Expression.CompileToFunction(context, command);
            }
            else if (this.Subquery != null) {
                lhsFunction = this.Subquery.CompileToFunction(context, command);
            }
            else {
                throw new QueryCompilationException("Expression and Subquery can't all be null in a WInPredicate.");
            }

            List<ScalarFunction> values = this.Values.Select(value => value.CompileToFunction(context, command)).ToList();

            return new InFunction(lhsFunction, values, this.NotDefined);
        }
    }

    public partial class WBooleanNotExpression
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            return new BooleanNotFunction(Expression.CompileToFunction(context, command));
        }

        internal override BooleanFunction CompileToBatchFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            return new BooleanNotFunction(this.Expression.CompileToBatchFunction(context, command));
        }
    }

    public partial class WBooleanParenthesisExpression
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            return this.Expression.CompileToFunction(context, command);
        }

        internal override BooleanFunction CompileToBatchFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            return this.Expression.CompileToBatchFunction(context, command);
        }
    }

    public partial class WExistsPredicate
    {
        internal override BooleanFunction CompileToFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            QueryCompilationContext subContext = new QueryCompilationContext(context);
            Container container = new Container();
            subContext.OuterContextOp.SetContainer(container);
            GraphViewExecutionOperator subQueryOp = Subquery.SubQueryExpr.Compile(subContext, command);
            ExistsFunction existsFunc = new ExistsFunction(subQueryOp, container);

            return existsFunc;
        }

        internal override BooleanFunction CompileToBatchFunction(QueryCompilationContext context, GraphViewCommand command)
        {
            QueryCompilationContext subContext = new QueryCompilationContext(context);
            Container container = new Container();
            subContext.OuterContextOp.SetContainer(container);
            subContext.AddField(GremlinKeyword.IndexTableName, command.IndexColumnName, ColumnGraphType.Value, true);
            subContext.InBatchMode = true;

            GraphViewExecutionOperator subQueryOp = this.Subquery.SubQueryExpr.Compile(subContext, command);
            ExistsFunction existsFunc = new ExistsFunction(subQueryOp, container);

            return existsFunc;
        }
    }
}
