using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinCountOp: GremlinTranslationOperator
    {
        public GremlinCountOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WScalarExpression parameter = GremlinUtil.GetStarColumnReferenceExpression(); //TODO

            inputContext.ProcessProjectWithFunctionCall(Labels, "count", parameter);

            return inputContext;
        }
    }
}
