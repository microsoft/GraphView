using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatOp: GremlinTranslationOperator
    {
        public Predicate ConditionPredicate { get; set; }
        public GraphTraversal2 RepeatTraversal { get; set; }
        public GraphTraversal2 ConditionTraversal { get; set; }
        public RepeatCondition RepeatCondition { get; set; }

        public GremlinRepeatOp(GraphTraversal2 repeatTraversal)
        {
            RepeatTraversal = repeatTraversal;
            RepeatCondition = new RepeatCondition();
        }

        public GremlinRepeatOp()
        {
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedVariableFromParent(RepeatTraversal, inputContext);
            GremlinToSqlContext repeatContext = RepeatTraversal.GetEndOp().GetContext();

            GremlinToSqlContext conditionContext = null;
            if (ConditionPredicate != null)
            {
                throw new NotImplementedException();
            }
            else
            {
                GremlinUtil.InheritedVariableFromParent(ConditionTraversal, repeatContext);
                conditionContext = ConditionTraversal.GetEndOp().GetContext();

                inputContext.PivotVariable.Repeat(inputContext, repeatContext, conditionContext, RepeatCondition);
            }

            return inputContext;
        }

    }

    internal class RepeatCondition
    {
        public bool IsEmitTrue { get; set; }
        public bool IsEmitBefore { get; set; }
        public bool IsEmitAfter { get; set; }
        public bool IsUntilBefore { get; set; }
        public bool IsUntilAfter { get; set; }
        public bool IsTimes { get; set; }
        public long Times { get; set; }

        public RepeatCondition() { }
    }
}
