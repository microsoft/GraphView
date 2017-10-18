using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCommitOp : GremlinTranslationOperator
    {
        public int MaxBarrierSize { get; set; }

        public GremlinCommitOp()
        {
        }

        public GremlinCommitOp(int maxBarrierSize)
        {
            MaxBarrierSize = maxBarrierSize;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            //maxBarrierSize is useless for our runtime, so we won't use this parameter
            inputContext.PivotVariable.Commit(inputContext);

            return inputContext;
        }
    }
}
