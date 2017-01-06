using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCountOp: GremlinTranslationOperator
    {
        public GremlinCountOp() { }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Count(inputContext);

            return inputContext;
        }
    }
}
