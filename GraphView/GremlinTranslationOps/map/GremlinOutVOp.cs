using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinOutVOp: GremlinTranslationOperator
    {
        public GremlinOutVOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.CheckIsGremlinEdgeVariable(inputContext.CurrVariable);
            var existInPath = inputContext.Paths.Find(p => p.Item2 == inputContext.CurrVariable);

            inputContext.SetCurrVariable(existInPath.Item1);

            return inputContext;
        }
    }
}
