using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinEOp: GremlinTranslationOperator
    {
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVertexVariable sourceVertex = new GremlinVertexVariable();
            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable();
            GremlinVertexVariable sinkVertex = new GremlinVertexVariable();
            inputContext.AddNewVariable(sourceVertex);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddNewVariable(sinkVertex);
            inputContext.SetCurrVariable(newEdgeVar);
            inputContext.SetDefaultProjection(newEdgeVar);
            inputContext.AddPaths(sourceVertex, newEdgeVar, sinkVertex);
           
            return inputContext;
        }

    }
}
