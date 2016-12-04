using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.branch
{
    internal class GremlinRepeatOp: GremlinTranslationOperator
    {
        public int MaxLoops;
        public GraphTraversal2 RepeatTraversal;
        public GraphTraversal2 UntilTraversal;
        public GraphTraversal2 EmitTraversal;
        public Predicate UntilPredicate;
        public Predicate EmitPredicate;

        public GremlinRepeatOp(GraphTraversal2 repeatTraversal)
        {
            RepeatTraversal = repeatTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedVariableFromParent(RepeatTraversal, inputContext);

            WRepeat repeatExpr = new WRepeat()
            {
                SqlStatement = RepeatTraversal.GetEndOp().GetContext().ToSelectQueryBlock()
            };

            if (MaxLoops != null)
            {
                repeatExpr.MaxLoops = MaxLoops;
            }
            if (UntilTraversal != null)
            {
                repeatExpr.UntilCondition =
                    GremlinUtil.GetExistPredicate(UntilTraversal.GetEndOp().GetContext().ToSqlStatement());
            }
            if (UntilPredicate != null)
            {
                //TODO
            }

            return inputContext;
        }

    }
}
