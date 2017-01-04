using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinScalarSubquery : GremlinScalarVariable
    {
        public GremlinToSqlContext SubqueryContext { get; private set; }

        public GremlinScalarSubquery(GremlinToSqlContext subqueryContext)
        {
            SubqueryContext = subqueryContext;
        }
    }
}
