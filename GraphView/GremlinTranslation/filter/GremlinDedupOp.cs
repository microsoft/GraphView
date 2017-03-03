using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinDedupOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public List<string> DedupLabels { get; set; }
        public GraphTraversal2 ByTraversal { get; set; }

        public GremlinDedupOp(params string[] dedupLabels)
        {
            DedupLabels = new List<string>(dedupLabels);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinToSqlContext dedupContext = null;
            if (ByTraversal != null)
            {
                ByTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                dedupContext = ByTraversal.GetEndOp().GetContext();
            }

            inputContext.PivotVariable.Dedup(inputContext, DedupLabels, dedupContext);

            return inputContext;
        }

        public override void ModulateBy()
        {
            ByTraversal = GraphTraversal2.__();
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByTraversal = traversal;
        }

        public override void ModulateBy(string key)
        {
            ByTraversal = GraphTraversal2.__().Values(key);
        }
    }
}
