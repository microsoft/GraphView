using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinMaxOp: GremlinTranslationOperator
    {
        public Scope Scope;
        public GremlinMaxOp() { }
        public GremlinMaxOp(Scope scope)
        {
            Scope = scope;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("count"));
            return new GremlinToSqlContext();
        }

        public override WSqlFragment ToWSqlFragment()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("max"));
            return inputContext.ToSqlQuery();
        }
    }
}
