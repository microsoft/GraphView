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

            var existInPath = inputContext.NewPathList.Find(p => p.SourceVariable.VariableName == inputContext.CurrVariable.VariableName || p.SinkVariable.VariableName == inputContext.CurrVariable.VariableName);

            if (existInPath.SourceVariable == inputContext.CurrVariable)
            {
                inputContext.SetCurrVariable(existInPath.SinkVariable);
            }
            if (existInPath.SinkVariable == inputContext.CurrVariable)
            {
                inputContext.SetCurrVariable(existInPath.SourceVariable);
            }
            
            return inputContext;
        }
    }
}
