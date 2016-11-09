using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinCountOp: GremlinTranslationOperator
    {
        public GremlinCountOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("count"));

            return new GremlinToSqlContext();
        }

        public override WSqlFragment ToWSqlFragment()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("count"));
            return inputContext.ToSqlQuery();
        }
    }
}
