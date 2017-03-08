using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMatchPath
    {
        public GremlinFreeVertexVariable SourceVariable { get; set; } 
        public GremlinFreeEdgeVariable EdgeVariable { get; set; } 
        public GremlinFreeVertexVariable SinkVariable { get; set; } 
 
        public GremlinMatchPath(GremlinFreeVertexVariable sourceVariable, GremlinFreeEdgeVariable edgeVariable, GremlinFreeVertexVariable sinkVariable)
        { 
            SourceVariable = sourceVariable; 
            EdgeVariable = edgeVariable; 
            SinkVariable = sinkVariable; 
        }
    }
}
