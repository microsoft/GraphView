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
            RepeatCondition = new RepeatCondition();
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

            if (ConditionTraversal != null)
            {
                GremlinUtil.InheritedVariableFromParent(ConditionTraversal, repeatContext);
                conditionContext = ConditionTraversal.GetEndOp().GetContext();
                RepeatCondition.ConditionBooleanExpression = conditionContext.ToSqlBoolean();
            }

            inputContext.PivotVariable.Repeat(inputContext, repeatContext, RepeatCondition);

            return inputContext;
        }

    }

    internal class RepeatCondition
    {
        public WBooleanExpression ConditionBooleanExpression { get; set; }
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
