using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOutVOp: GremlinTranslationOperator
    {
        public GremlinOutVOp() {}

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of outV()-step can't be null.");
            }

            inputContext.PivotVariable.OutV(inputContext);

            return inputContext;
        }
    }
}
