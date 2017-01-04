using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectVariable: GremlinScalarVariable
    {
        public List<string> ProjectKeys { get; set; }
        public List<GremlinToSqlContext> ProjectContextList { get; set; }

        public GremlinProjectVariable(List<string> projectKeys)
        {
            ProjectKeys = new List<string>(projectKeys);
            ProjectContextList = new List<GremlinToSqlContext>();
        }

        internal override void By(GremlinToSqlContext currentContext, GremlinToSqlContext byContext)
        {
            ProjectContextList.Add(byContext);
        }

    }
}
