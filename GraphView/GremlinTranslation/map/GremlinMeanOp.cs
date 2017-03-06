using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMeanOp: GremlinTranslationOperator
    {
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinMeanOp(GremlinKeyword.Scope scope)
        {
            Scope = scope;
        }
        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (Scope == GremlinKeyword.Scope.global)
            {
                inputContext.PivotVariable.Mean(inputContext);
            }
            else
            {
                inputContext.PivotVariable.MeanLocal(inputContext);
            }

            return inputContext;
        }
    }
}
