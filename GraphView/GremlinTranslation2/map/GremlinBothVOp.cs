using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBothVOp: GremlinTranslationOperator
    {
        public GremlinBothVOp() { }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.BothV(inputContext);

            return inputContext;
        }
    }
}
