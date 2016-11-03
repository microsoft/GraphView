using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GramlinTranslationOperator
{
    internal class GremlinVOp: GremlinTranslationOperator
    {
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVertexVariable newVariable = new GremlinVertexVariable();
            inputContext.RemainingVariableList.Add(newVariable);
            inputContext.Projection.Add(new Tuple<GremlinVariable, string>(newVariable, "id"));
            return inputContext;
        }
    }
}
