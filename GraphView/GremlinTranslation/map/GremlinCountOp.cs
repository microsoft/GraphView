using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCountOp: GremlinTranslationOperator
    {
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinCountOp(GremlinKeyword.Scope scope)
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
                inputContext.PivotVariable.Count(inputContext);
            }
            else
            {
                inputContext.PivotVariable.CountLocal(inputContext);
            }

            return inputContext;
        }
    }
}
