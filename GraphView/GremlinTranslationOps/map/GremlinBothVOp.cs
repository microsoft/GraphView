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

            var currEdge = inputContext.CurrVariable;
            GremlinUtil.CheckIsGremlinEdgeVariable(currEdge);

            var existInPath = inputContext.Paths.Find(p => p.Item2 == currEdge);

            GremlinJoinVertexVariable newVariable = new GremlinJoinVertexVariable(existInPath.Item1, existInPath.Item3);
            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetDefaultProjection(newVariable);
            inputContext.SetCurrVariable(newVariable);
            
            return inputContext;
        }
    }
}
