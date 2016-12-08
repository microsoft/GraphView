using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinTreeOp: GremlinTranslationOperator
    {
        public GremlinTreeOp()
        {
            
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WScalarExpression parameter = GremlinUtil.GetStarColumnReferenceExpression(); //TODO

            inputContext.ProcessProjectWithFunctionCall(Labels, "tree", parameter);

            return inputContext;
        }
    }
}
