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
        public GraphTraversal2 RepeatTraversal { get; set; }
        public Predicate ConditionPredicate { get; set; }
        public GraphTraversal2 ConditionTraversal { get; set; }
        public bool IsEmitTrue { get; set; }
        public bool IsEmitBefore { get; set; }
        public bool IsEmitAfter { get; set; }
        public bool IsUntilBefore { get; set; }
        public bool IsUntilAfter { get; set; }
        public bool IsTimes { get; set; }
        public long Times { get; set; }

        public GremlinRepeatOp(GraphTraversal2 repeatTraversal)
        {
            RepeatTraversal = repeatTraversal;
        }

        public GremlinRepeatOp()
        {
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Repeat(inputContext, this);

            return inputContext;
        }

    }
}
