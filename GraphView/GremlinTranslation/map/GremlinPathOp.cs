using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathOp : GremlinTranslationOperator
    {
        public List<GraphTraversal2> ByList { get; set; }

        public GremlinPathOp()
        {
            ByList = new List<GraphTraversal2>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (ByList.Count == 0)
            {
                ByList.Add(GraphTraversal2.__());
            }

            inputContext.PivotVariable.Path(inputContext, ByList);

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByList.Add(traversal);
        }

        public override void ModulateBy(string key)
        {
            ByList.Add(GraphTraversal2.__().Values(key));
        }

        public override void ModulateBy()
        {
            ByList.Add(GraphTraversal2.__());
        }
    }
}
