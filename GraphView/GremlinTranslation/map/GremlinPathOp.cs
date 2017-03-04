using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPathOp : GremlinTranslationOperator
    {
        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Path(inputContext);

            return inputContext;
        }
    }

    internal class GremlinPath2Op : GremlinTranslationOperator
    {
        public List<object> ByList { get; set; }

        public GremlinPath2Op()
        {
            ByList = new List<object>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (ByList.Count == 0)
            {
                ByList.Add(inputContext.PivotVariable.GetProjectKey());
            }

            List<object> byList = new List<object>();
            foreach (var item in ByList)
            {
                if (item is GraphTraversal2)
                {
                    (item as GraphTraversal2).GetStartOp().InheritedVariableFromParent(inputContext);
                    byList.Add((item as GraphTraversal2).GetEndOp().GetContext());
                }
                else if (item is string)
                {
                    byList.Add(item);
                }
                else
                {
                    throw new QueryCompilationException("Can't process this type : " + item.GetType());
                }
            }

            inputContext.PivotVariable.Path2(inputContext, byList);

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByList.Add(traversal);
        }

        public override void ModulateBy(string key)
        {
            ByList.Add(key);
        }
    }
}
