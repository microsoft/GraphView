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
        public GraphTraversal2 ByTraversal { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinDedupOp(GremlinKeyword.Scope scope, params string[] dedupLabels)
        {
            DedupLabels = new List<string>(dedupLabels);
            Scope = scope;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            GremlinToSqlContext dedupContext = null;
            if (ByTraversal != null)
            {
                ByTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                dedupContext = ByTraversal.GetEndOp().GetContext();
            }

            // Dedup(local, "x", "y"), the dedupLabels should be ignored
            if (Scope == GremlinKeyword.Scope.local) DedupLabels.Clear();

            inputContext.PivotVariable.Dedup(inputContext, DedupLabels, dedupContext, Scope);

            return inputContext;
        }

        public override void ModulateBy()
        {
            if (Scope == GremlinKeyword.Scope.local) throw new SyntaxErrorException("Dedup(local) can't be modulated by by()");
            ByTraversal = GraphTraversal2.__();
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            if (Scope == GremlinKeyword.Scope.local) throw new SyntaxErrorException("Dedup(local) can't be modulated by by()");
            ByTraversal = traversal;
        }

        public override void ModulateBy(string key)
        {
            if (Scope == GremlinKeyword.Scope.local) throw new SyntaxErrorException("Dedup(local) can't be modulated by by()");
            ByTraversal = GraphTraversal2.__().Values(key);
        }
    }
}
