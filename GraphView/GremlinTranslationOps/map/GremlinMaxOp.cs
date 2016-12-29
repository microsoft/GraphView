using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMaxOp: GremlinTranslationOperator
    {
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinMaxOp() { }

        public GremlinMaxOp(GremlinKeyword.Scope scope)
        {
            Scope = scope;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<WScalarExpression> parameterList = new List<WScalarExpression>() { GremlinUtil.GetStarColumnReferenceExpression() };

            inputContext.ProcessProjectWithFunctionCall(Labels, "max", parameterList);

            return inputContext;
        }
    }
}
