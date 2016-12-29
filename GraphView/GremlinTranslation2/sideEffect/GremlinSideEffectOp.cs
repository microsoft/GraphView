using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSideEffectOp: GremlinTranslationOperator
    {
        public GraphTraversal2 SideEffectTraversal { get; set; }

        public GremlinSideEffectOp(GraphTraversal2 sideEffectTraversal)
        {
            SideEffectTraversal = sideEffectTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedContextFromParent(SideEffectTraversal, inputContext);

            inputContext.SaveCurrentState();
            GremlinToSqlContext context = SideEffectTraversal.GetEndOp().GetContext();
            WSqlStatement statement = context.ToSqlStatement();
            //add statement if it's not a selectqueryblock, skip this statement if it's a selectqueryblock statement
            if (!(statement is WSelectQueryBlock))
            {
                inputContext.Statements.Add(statement);
            }
            inputContext.ResetSavedState();

            return inputContext;

        }
    }
}
