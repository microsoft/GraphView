using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMatchPath
    { 
        public GremlinVertexVariable SourceVariable { get; set; } 
        public GremlinEdgeVariable EdgeVariable { get; set; } 
        public GremlinVertexVariable SinkVariable { get; set; } 
 
        public GremlinMatchPath(GremlinVertexVariable sourceVariable, GremlinEdgeVariable edgeVariable, GremlinVertexVariable sinkVariable)
        { 
            SourceVariable = sourceVariable; 
            EdgeVariable = edgeVariable; 
            SinkVariable = sinkVariable; 
        } 
    } 
}
