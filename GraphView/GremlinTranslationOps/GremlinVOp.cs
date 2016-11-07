using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinVOp: GremlinTranslationOperator
    {
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVertexVariable newVertexVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(newVertexVar);
            inputContext.SetCurrentVariable(newVertexVar);
            inputContext.AddNewDefaultProjection(newVertexVar);
            return inputContext;
        }
    }
}
