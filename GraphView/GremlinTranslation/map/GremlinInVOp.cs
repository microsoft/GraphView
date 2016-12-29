using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinInVOp: GremlinTranslationOperator
    {
        public GremlinInVOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinVariable inVariable = null;

            if (inputContext.CurrVariable is GremlinAddEVariable)
            {
                inVariable = (inputContext.CurrVariable as GremlinAddEVariable).ToVariable;
            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                inVariable = inputContext.GetSinkNode(inputContext.CurrVariable);
                if (inVariable == null)
                {
                    inputContext.CurrVariable.Properties.Add("_sink");
                    inVariable = new GremlinVirtualVertexVariable(inputContext.CurrVariable as GremlinEdgeVariable);
                    inputContext.AddNewVariable(inVariable);
                }
            }
            
            inputContext.SetCurrVariable(inVariable);
            inputContext.SetDefaultProjection(inVariable);

            return inputContext;
        }
    }
}
