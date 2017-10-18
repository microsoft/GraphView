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
        public GraphTraversal ByTraversal { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinDedupOp(GremlinKeyword.Scope scope, params string[] dedupLabels)
        {
            DedupLabels = new List<string>(dedupLabels);
            Scope = scope;
            ByTraversal = GraphTraversal.__();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of dedup()-step can't be null.");
            }

            // Dedup(Local, "x", "y"), the dedupLabels should be ignored
            if (Scope == GremlinKeyword.Scope.Local) DedupLabels.Clear();

            inputContext.PivotVariable.Dedup(inputContext, DedupLabels, ByTraversal, Scope);

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal traversal)
        {
            if (Scope == GremlinKeyword.Scope.Local)
            {
                throw new SyntaxErrorException("Dedup(Local) can't be modulated by by()");
            }

            ByTraversal = traversal;
        }
    }
}
