using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOutVOp: GremlinTranslationOperator
    {
        public GremlinOutVOp() { }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("This step should follow by a edge variable.");
            }

            inputContext.PivotVariable.OutV(inputContext);

            return inputContext;
        }
    }
}
