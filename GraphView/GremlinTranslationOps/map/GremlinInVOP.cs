using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinInVOp: GremlinTranslationOperator
    {
        public GremlinInVOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.CheckIsGremlinEdgeVariable(inputContext.CurrVariable);
            var existInPath = inputContext.Paths.Find(p => p.Item2 == inputContext.CurrVariable);
                inputContext.SetCurrentVariable(existInPath.Item3);

            return inputContext;
        }
    }
}
