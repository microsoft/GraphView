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
        public GremlinKeyword.Scope Scope { get; set; }
        public bool IsReverse { get; set; }

        public GremlinRangeOp(int low, int high, GremlinKeyword.Scope scope, bool isReverse = false)
        {
            Low = low;
            High = high;
            Scope = scope;
            IsReverse = isReverse;
        }
        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Range(inputContext, Low, High, Scope, IsReverse);

            return inputContext;
        }
    }
}
