using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GremlinTranslationOps.filter;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinSideEffectOp: GremlinTranslationOperator
    {
        public GraphTraversal2 SideEffectTraversal;

        public GremlinSideEffectOp(GraphTraversal2 sideEffectTraversal)
        {
            SideEffectTraversal = sideEffectTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedVariableFromParent(SideEffectTraversal, inputContext);

            WSideEffect sideEffect = new WSideEffect()
            {
                SqlStatement = SideEffectTraversal.GetEndOp().GetContext().ToSqlQuery()
            };

            GremlinSideEffectVariable newVariable = new GremlinSideEffectVariable(sideEffect);
            inputContext.AddNewVariable(newVariable, Labels);

            return inputContext;

        }
    }
}
