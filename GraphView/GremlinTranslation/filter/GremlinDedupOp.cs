using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslation
{
    internal class GremlinDedupOp: GremlinTranslationOperator
    {
        public List<string> DedupLabels { get; set; }

        public GremlinDedupOp(params string[] dedupLabels)
        {
            DedupLabels = new List<string>();
            foreach (var dedupLabel in dedupLabels)
            {
                DedupLabels.Add(dedupLabel);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<WScalarExpression> parameterList = new List<WScalarExpression>() { GremlinUtil.GetStarColumnReferenceExpression() }; //TODO

            inputContext.ProcessProjectWithFunctionCall(Labels, "dedup", parameterList);

            return inputContext;
        }
    }
}
