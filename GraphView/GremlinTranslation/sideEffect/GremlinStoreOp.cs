using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinStoreOp: GremlinTranslationOperator
    {
        public string SideEffectKey { get; set; }

        public GremlinStoreOp(string sideEffectKey)
        {
            SideEffectKey = sideEffectKey;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.Store(inputContext, SideEffectKey);

            return inputContext;
        }
    }
}
