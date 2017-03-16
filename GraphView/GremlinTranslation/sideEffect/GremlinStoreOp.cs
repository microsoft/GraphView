using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinStoreOp: GremlinTranslationOperator
    {
        public string SideEffectKey { get; set; }
        public GraphTraversal2 ByTraversal { get; set; }

        public GremlinStoreOp(string sideEffectKey)
        {
            SideEffectKey = sideEffectKey;
            ByTraversal = GraphTraversal2.__();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            ByTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            inputContext.PivotVariable.Store(inputContext, SideEffectKey, ByTraversal.GetEndOp().GetContext());

            return inputContext;
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByTraversal = traversal;
        }
    }
}
