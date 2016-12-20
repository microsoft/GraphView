using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinOtherVOp: GremlinTranslationOperator
    {
        public GremlinOtherVOp()
        {
            
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            var existInPath = inputContext.NewPathList.Find(p => p.Item1.VariableName == inputContext.CurrVariable.VariableName || p.Item3.VariableName == inputContext.CurrVariable.VariableName);

            if (existInPath.Item1 == inputContext.CurrVariable)
            {
                inputContext.SetCurrVariable(existInPath.Item3);
            }
            if (existInPath.Item3 == inputContext.CurrVariable)
            {
                inputContext.SetCurrVariable(existInPath.Item1);
            }
            
            return inputContext;
        }
    }
}
