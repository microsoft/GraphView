using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMatchPath
    {
        public GremlinVertexTableVariable SourceVariable { get; set; } 
        public GremlinEdgeTableVariable EdgeVariable { get; set; } 
        public GremlinVertexTableVariable SinkVariable { get; set; } 
 
        public GremlinMatchPath(GremlinVertexTableVariable sourceVariable, GremlinEdgeTableVariable edgeVariable, GremlinVertexTableVariable sinkVariable)
        { 
            SourceVariable = sourceVariable; 
            EdgeVariable = edgeVariable; 
            SinkVariable = sinkVariable; 
        } 
    } 
}
