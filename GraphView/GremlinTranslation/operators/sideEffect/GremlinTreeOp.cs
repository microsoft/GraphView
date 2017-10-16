using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinTreeOp: GremlinTranslationOperator
    {
        public string SideEffectKey { get; set; }

        public List<GraphTraversal> ByList { get; set; }

        public GremlinTreeOp(string sideEffectKey)
        {
            SideEffectKey = sideEffectKey;
            ByList = new List<GraphTraversal>();
        }

        public GremlinTreeOp()
        {
            ByList = new List<GraphTraversal>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of tree()-step can't be null.");
            }

            if (SideEffectKey == null)
                inputContext.PivotVariable.Tree(inputContext, ByList);
            else
                inputContext.PivotVariable.Tree(inputContext, SideEffectKey, ByList);
            return inputContext;
        }

        public override void ModulateBy(GraphTraversal traversal)
        {
            ByList.Add(traversal);
        }
    }
}
