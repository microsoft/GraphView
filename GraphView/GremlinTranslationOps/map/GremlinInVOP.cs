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

            inputContext.ClearCurrentVariable();
            foreach (var currEdge in inputContext.CurrVariableList)
            {
                GremlinUtil.CheckIsGremlinEdgeVariable(currEdge);
                var ExistInPath = inputContext.Paths.Find(p => p.Item2 == currEdge);
                inputContext.AddCurrentVariable(ExistInPath.Item3);
            }
            return inputContext;
        }
    }
}
