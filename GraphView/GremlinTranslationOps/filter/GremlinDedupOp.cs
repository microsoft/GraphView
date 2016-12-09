using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinDedupOp: GremlinTranslationOperator
    {
        public List<string> DedupLabels;

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

            WScalarExpression parameter = GremlinUtil.GetStarColumnReferenceExpression(); //TODO

            inputContext.ProcessProjectWithFunctionCall(Labels, "dedup", parameter);

            return inputContext;
        }
    }
}
