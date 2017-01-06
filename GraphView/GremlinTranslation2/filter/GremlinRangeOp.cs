using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRangeOp: GremlinTranslationOperator
    {
        public long Low { get; set; }
        public long High { get; set; }
        public GremlinRangeOp(long low, long high)
        {
            Low = low;
            High = high;
        }
        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Range(inputContext, Low, High);

            return inputContext;
        }
    }
}
