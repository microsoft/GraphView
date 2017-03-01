using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAggregateOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public string SideEffectKey { get; set; }
        public GraphTraversal2 ByTraversal { get; set; }

        public GremlinAggregateOp(string sideEffectKey)
        {
            SideEffectKey = sideEffectKey;
            ByTraversal = GraphTraversal2.__();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            ByTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            inputContext.PivotVariable.Aggregate(inputContext, SideEffectKey, ByTraversal.GetEndOp().GetContext());

            return inputContext;
        }

        public void ModulateBy()
        {
            throw new NotImplementedException();
        }

        public void ModulateBy(GraphTraversal2 traversal)
        {
            ByTraversal = traversal;
        }

        public void ModulateBy(string key)
        {
            ByTraversal = GraphTraversal2.__().Values(key);
        }

        public void ModulateBy(GremlinKeyword.Order order)
        {
            throw new NotImplementedException();
        }
    }
}
