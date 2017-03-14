using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinSampleOp: GremlinTranslationOperator
    {
        public GremlinKeyword.Scope Scope { get; set; }
        public int AmountToSample { get; set; }
        public GraphTraversal2 ProbabilityTraversal { get; set; }

        public GremlinSampleOp(GremlinKeyword.Scope scope, int amountToSample)
        {
            Scope = scope;
            AmountToSample = amountToSample;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            GremlinToSqlContext probabilityContext = null;
            if (ProbabilityTraversal != null)
            {
                ProbabilityTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                probabilityContext = ProbabilityTraversal.GetEndOp().GetContext();
            }

            inputContext.PivotVariable.Sample(inputContext, Scope, AmountToSample, probabilityContext);

            return inputContext;
        }

        public override void ModulateBy()
        {
            if (Scope == GremlinKeyword.Scope.local) throw new SyntaxErrorException("Sample(local) can't be modulated by by()");
            ProbabilityTraversal = GraphTraversal2.__();
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            if (Scope == GremlinKeyword.Scope.local) throw new SyntaxErrorException("Sample(local) can't be modulated by by()");
            ProbabilityTraversal = traversal;
        }

        public override void ModulateBy(string key)
        {
            if (Scope == GremlinKeyword.Scope.local) throw new SyntaxErrorException("Sample(local) can't be modulated by by()");
            ProbabilityTraversal = GraphTraversal2.__().Values(key);
        }
    }
}
