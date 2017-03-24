using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectOp: GremlinTranslationOperator
    {
        public List<string> SelectKeys { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }
        public List<GraphTraversal2> ByList { get; set; }

        public GremlinSelectOp(GremlinKeyword.Pop pop, params string[] selectKeys)
        {
            SelectKeys = new List<string>(selectKeys);
            Pop = pop;
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

            inputContext.PivotVariable.Select(inputContext, Pop, SelectKeys, ByList);

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByList.Add(traversal);
        }
    }
}
