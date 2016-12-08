using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinSumOp: GremlinTranslationOperator
    {
        public GremlinSumOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WScalarExpression parameter = GremlinUtil.GetStarColumnReferenceExpression(); //TODO

            inputContext.ProcessProjectWithFunctionCall(Labels, "sum", parameter);
            return inputContext;
        }
    }
}