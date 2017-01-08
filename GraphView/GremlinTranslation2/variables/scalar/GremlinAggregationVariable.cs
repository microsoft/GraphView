using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinAggregationVariable : GremlinScalarVariable
    {
        public GremlinScalarVariable AggregateProjection { get; set; }

        public GremlinAggregationVariable(GremlinScalarVariable aggregateProjection)
        {
            AggregateProjection = aggregateProjection;
        }
    }

    internal class GremlinTreeVariable : GremlinScalarVariable
    {
        public override WScalarExpression ToScalarExpression()
        {
            return SqlUtil.GetFunctionCall("tree");
        }
    }
}
