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
            SinkVariable = sinkVariable;
        }

        public void SetSourceVariable(GremlinTableVariable sourceVariable)
        {
            SourceVariable = sourceVariable;
        }
    } 
}
