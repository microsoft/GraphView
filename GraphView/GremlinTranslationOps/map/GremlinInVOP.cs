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
                    inVariable = new GremlinVertexVariable();
                    inputContext.SetSinkNode(inputContext.CurrVariable, inVariable);
                    if (inputContext.NewVariableList.Count == 0)
                    {
                        inputContext.AddPaths(inputContext.GetSourceNode(inputContext.CurrVariable),
                                              inputContext.CurrVariable,
                                              inputContext.GetSinkNode(inputContext.CurrVariable));
                    }
                    inputContext.AddNewVariable(inVariable);
                }
            }
            
            inputContext.SetCurrVariable(inVariable);
            inputContext.SetDefaultProjection(inVariable);

            return inputContext;
        }
    }
}
