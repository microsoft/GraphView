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
        public List<GraphTraversal2> TraversalList { get; set; }

        public GremlinProjectOp(params string[] projectKeys)
        {
            TraversalList = new List<GraphTraversal2>();
            ProjectKeys = new List<string>();
            foreach (var projectKey in projectKeys)
            {
                ProjectKeys.Add(projectKey);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }
        public void ModulateBy()
        {

        }
        public void ModulateBy(GraphTraversal2 traversal)
        {
            TraversalList.Add(traversal);
        }

        public void ModulateBy(string key)
        {
        }

        public void ModulateBy(GremlinKeyword.Order order)
        {
        }
    }
}
