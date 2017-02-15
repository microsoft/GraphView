using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinDedupOp: GremlinTranslationOperator
    {
        public List<string> DedupLabels { get; set; }

        public GremlinDedupOp(params string[] dedupLabels)
        {
            DedupLabels = new List<string>(dedupLabels);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Dedup(inputContext, DedupLabels);

            return inputContext;
        }
    }
}
