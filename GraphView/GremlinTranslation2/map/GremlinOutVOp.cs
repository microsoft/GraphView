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

            inputContext.PivotVariable.OutV(inputContext);

            return inputContext;
        }
    }
}
