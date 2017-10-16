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
        public List<GraphTraversal> ByGraphTraversal { get; set; }

        public GremlinProjectOp(params string[] projectKeys)
        {
            ProjectKeys = new List<string>(projectKeys);
            ByGraphTraversal = new List<GraphTraversal>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of project()-step can't be null.");
            }

            if (ByGraphTraversal.Count == 0)
            {
                ByGraphTraversal.Add(GraphTraversal.__());
            }

            if (ByGraphTraversal.Count > ProjectKeys.Count)
            {
                ByGraphTraversal.RemoveRange(ProjectKeys.Count, ByGraphTraversal.Count - ProjectKeys.Count);
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

        public override void ModulateBy(GraphTraversal traversal)
        {
            ByGraphTraversal.Add(traversal);
        }
    }
}
