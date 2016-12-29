using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinSumOp: GremlinTranslationOperator
    {
        public GremlinSumOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<WScalarExpression> parameterList = new List<WScalarExpression>() { GremlinUtil.GetStarColumnReferenceExpression() }; //TODO

            inputContext.ProcessProjectWithFunctionCall(Labels, "sum", parameterList);
            return inputContext;
        }
    }
}