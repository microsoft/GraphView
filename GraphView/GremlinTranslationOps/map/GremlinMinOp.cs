using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinMinOp: GremlinTranslationOperator
    {
        public Scope Scope;
        public GremlinMinOp() { }

        public GremlinMinOp(Scope scope)
        {
            Scope = scope;
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            List<string> parameters = new List<string>();

            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("min"));
            return new GremlinToSqlContext();
        }
        public override WSqlFragment ToWSqlFragment()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("min"));
            return inputContext.ToSqlQuery();
        }
    }
}
