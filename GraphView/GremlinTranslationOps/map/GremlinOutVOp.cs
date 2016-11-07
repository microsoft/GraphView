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

            inputContext.ClearCurrentVariable();
            foreach (var currEdge in inputContext.CurrVariableList)
            {
                GremlinUtil.CheckIsGremlinEdgeVariable(currEdge);
                var ExistInPath = inputContext.Paths.Find(p => p.Item2 == currEdge);
                inputContext.SetCurrentVariable(ExistInPath.Item1);
            }
            return inputContext;
        }
    }
}
