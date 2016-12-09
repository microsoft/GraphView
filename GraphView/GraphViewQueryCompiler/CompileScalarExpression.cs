using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    public partial class WScalarExpression
    {
        internal virtual ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            return null;
        }
    }

    public partial class WScalarSubquery
    {
        internal override ScalarFunction CompileToFunction(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            QueryCompilationContext subContext = new QueryCompilationContext(context);
            GraphViewExecutionOperator subQueryOp = SubQueryExpr.Compile(subContext, dbConnection);
            return new ScalarSubqueryFunction(subQueryOp, subContext.outerContextOp);
        }
    }
}
