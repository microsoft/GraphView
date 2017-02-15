using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinEOp: GremlinTranslationOperator
    {
        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinFreeVertexVariable newVariable = new GremlinFreeVertexVariable();

            inputContext.VariableList.Add(newVariable);
            inputContext.TableReferences.Add(newVariable);
            inputContext.SetPivotVariable(newVariable);

            inputContext.PivotVariable.OutE(inputContext, new List<string>());

            return inputContext;
        }

    }
}
