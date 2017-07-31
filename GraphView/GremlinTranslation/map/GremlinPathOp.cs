using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathOp : GremlinTranslationOperator
    {
        public List<GraphTraversal> ByList { get; set; }
        public string FromLabel { get; set; }
        public string ToLabel { get; set; }

        public GremlinPathOp()
        {
            ByList = new List<GraphTraversal>();
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
                ByList.Add(GraphTraversal.__());
            }

            inputContext.PivotVariable.Path(inputContext, ByList, FromLabel, ToLabel);

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal traversal)
        {
            ByList.Add(traversal);
        }
    }
}
