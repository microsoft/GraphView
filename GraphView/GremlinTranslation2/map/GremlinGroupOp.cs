using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupOp: GremlinTranslationOperator
    {
        public string SideEffect { get; set; }

        public GremlinGroupOp()
        {
        }

        public GremlinGroupOp(string sideEffect)
        {
            SideEffect = sideEffect;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }
    }
}
