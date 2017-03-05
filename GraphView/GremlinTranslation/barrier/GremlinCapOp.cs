using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCapOp: GremlinTranslationOperator
    {
        public List<string> SideEffectKeys { get; set; }

        public GremlinCapOp(params string[] sideEffectKeys)
        {
            SideEffectKeys = new List<string>(sideEffectKeys);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }
            inputContext.PivotVariable.Cap(inputContext, SideEffectKeys);

            return inputContext;
        }
    }
}
