using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMinOp: GremlinTranslationOperator
    {
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinMinOp(GremlinKeyword.Scope scope)
        {
            Scope = scope;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (Scope == GremlinKeyword.Scope.global)
            {
                inputContext.PivotVariable.Min(inputContext);
            }
            else
            {
                inputContext.PivotVariable.MinLocal(inputContext);
            }

            return inputContext;
        }
    }
}
