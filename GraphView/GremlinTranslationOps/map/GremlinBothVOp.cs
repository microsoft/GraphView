using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinBothVOp: GremlinTranslationOperator
    {
        public GremlinBothVOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            var currEdge = inputContext.CurrVariableList.First();
            GremlinUtil.CheckIsGremlinEdgeVariable(currEdge);
            var ExistInPath = inputContext.Paths.Find(p => p.Item2 == currEdge);
            inputContext.SetCurrentVariable(ExistInPath.Item1, ExistInPath.Item3);
            
            return inputContext;
        }
    }
}
