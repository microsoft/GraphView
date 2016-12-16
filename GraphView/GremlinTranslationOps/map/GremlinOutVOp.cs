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

            //GremlinUtil.CheckIsGremlinEdgeVariable(inputContext.CurrVariable);
            GremlinVariable outVariable = null;

            if (inputContext.CurrVariable is GremlinAddEVariable)
            {
                outVariable = (inputContext.CurrVariable as GremlinAddEVariable).FromVariable;
            }
            else
            {
                outVariable = inputContext.PathList.Find(p => p.Item2 == inputContext.CurrVariable).Item3;
            }

            inputContext.SetCurrVariable(outVariable);
            inputContext.SetDefaultProjection(outVariable);

            return inputContext;
        }
    }
}
