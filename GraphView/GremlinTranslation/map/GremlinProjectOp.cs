using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectOp: GremlinTranslationOperator
    {
        public List<string> ProjectKeys { get; set; }
        public List<GraphTraversal2> ByGraphTraversal { get; set; }

        public GremlinProjectOp(params string[] projectKeys)
        {
            ProjectKeys = new List<string>(projectKeys);
            ByGraphTraversal = new List<GraphTraversal2>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            List<GremlinToSqlContext> byContexts = new List<GremlinToSqlContext>();
            foreach (var traversal in ByGraphTraversal)
            {
                traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                byContexts.Add(traversal.GetEndOp().GetContext());
            }

            inputContext.PivotVariable.Project(inputContext, ProjectKeys, byContexts);

            return inputContext;
        }
        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByGraphTraversal.Add(traversal);
        }
    }
}
