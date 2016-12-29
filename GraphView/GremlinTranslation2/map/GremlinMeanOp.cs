using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMeanOp: GremlinTranslationOperator
    {
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinMeanOp() { }

        public GremlinMeanOp(GremlinKeyword.Scope scope)
        {
            Scope = scope;
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<WScalarExpression> parameterList = new List<WScalarExpression>() { GremlinUtil.GetStarColumnReferenceExpression() }; //TODO

            inputContext.ProcessProjectWithFunctionCall(Labels, "mean", parameterList);

            return inputContext;
        }
    }
}
