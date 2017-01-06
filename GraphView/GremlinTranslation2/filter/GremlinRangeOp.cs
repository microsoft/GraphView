using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRangeOp: GremlinTranslationOperator
    {
        public int Low { get; set; }
        public int High { get; set; }
        public GremlinRangeOp(int low, int high)
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
