using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinVOp: GremlinTranslationOperator
    {
        public List<object> VertexIdsOrElements;
        public GremlinVOp(params object[] vertexIdsOrElements)
        {
            foreach (var vertexIdsOrElement in vertexIdsOrElements)
            {
                VertexIdsOrElements.Add(vertexIdsOrElement);
            }
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVertexVariable newVertexVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(newVertexVar, Labels);
            inputContext.SetCurrVariable(newVertexVar);
            inputContext.SetDefaultProjection(newVertexVar);
            return inputContext;
        }
    }
}
