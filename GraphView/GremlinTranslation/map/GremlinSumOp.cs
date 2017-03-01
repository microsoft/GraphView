using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSumOp: GremlinTranslationOperator
    {
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinSumOp(GremlinKeyword.Scope scope)
        {
            Scope = scope;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (Scope == GremlinKeyword.Scope.global)
            {
                inputContext.PivotVariable.Sum(inputContext);
            }
            else
            {
                inputContext.PivotVariable.SumLocal(inputContext);
            }

            return inputContext;
        }
    }
}