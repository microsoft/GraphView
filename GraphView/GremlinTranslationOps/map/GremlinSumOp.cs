using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinSumOp: GremlinTranslationOperator
    {
        public GremlinSumOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("sum"));

            return new GremlinToSqlContext();
        }

        public override WSqlFragment ToWSqlFragment()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("sum"));
            return inputContext.ToSqlQuery();
        }
    }
}