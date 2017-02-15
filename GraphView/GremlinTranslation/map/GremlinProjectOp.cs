using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectOp: GremlinTranslationOperator, IGremlinByModulating
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

            List<GremlinToSqlContext> byContexts = new List<GremlinToSqlContext>();
            foreach (var traversal in ByGraphTraversal)
            {
                traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                byContexts.Add(traversal.GetEndOp().GetContext());
            }

            inputContext.PivotVariable.Project(inputContext, ProjectKeys, byContexts);

            return inputContext;
        }

        public void ModulateBy()
        {
            throw new NotImplementedException();
        }

        public void ModulateBy(GraphTraversal2 traversal)
        {
            ByGraphTraversal.Add(traversal);
        }

        public void ModulateBy(string key)
        {
            throw new NotImplementedException();
        }

        public void ModulateBy(GremlinKeyword.Order order)
        {
            throw new NotImplementedException();
        }
    }
}
