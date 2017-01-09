using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMatchPath
    {
        public GremlinTableVariable SourceVariable { get; set; } 
        public GremlinTableVariable EdgeVariable { get; set; } 
        public GremlinTableVariable SinkVariable { get; set; } 
 
        public GremlinMatchPath(GremlinTableVariable sourceVariable, GremlinTableVariable edgeVariable, GremlinTableVariable sinkVariable)
        { 
            SourceVariable = sourceVariable; 
            EdgeVariable = edgeVariable; 
            SinkVariable = sinkVariable; 
        }

        public void SetSinkVariable(GremlinTableVariable sinkVariable)
        {
            if ((EdgeVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.InEdge)
            {
                throw new QueryCompilationException();
            }
            if ((EdgeVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.OutEdge)
            {
                SinkVariable = sinkVariable;
            }
        }

        public void SetSourceVariable(GremlinTableVariable sourceVariable)
        {
            if ((EdgeVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.InEdge)
            {
                SinkVariable = SinkVariable;
            }
            if ((EdgeVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.OutEdge)
            {
                throw new QueryCompilationException();
            }
        }
    } 
}
