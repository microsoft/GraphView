using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAggregateOp: GremlinTranslationOperator
    {
        public string SideEffectKey { get; set; }

        public GremlinAggregateOp(string sideEffectKey)
        {
            SideEffectKey = sideEffectKey;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Aggregate(inputContext, SideEffectKey);

            return inputContext;
        }
    }
}
