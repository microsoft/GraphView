using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMatchPath
    {
        public GremlinVariable SourceVariable { get; set; } 
        public GremlinVariable EdgeVariable { get; set; } 
        public GremlinVariable SinkVariable { get; set; } 
 
        public GremlinMatchPath(GremlinVariable sourceVariable, GremlinVariable edgeVariable, GremlinVariable sinkVariable)
        { 
            SourceVariable = sourceVariable; 
            EdgeVariable = edgeVariable; 
            SinkVariable = sinkVariable; 
        }

        public void SetSinkVariable(GremlinVariable sinkVariable)
        {
            SinkVariable = sinkVariable;
        }

        public void SetSourceVariable(GremlinVariable sourceVariable)
        {
            SourceVariable = sourceVariable;
        }

        public WMatchPath ToMatchPath()
        {
            if (!(SinkVariable is GremlinFreeVertexVariable))
            {
                SinkVariable = null;
            }
            if (!(SourceVariable is GremlinFreeVertexVariable))
            {
                SourceVariable = null;
            }
            return SqlUtil.GetMatchPath(this);
        }
    } 
}
