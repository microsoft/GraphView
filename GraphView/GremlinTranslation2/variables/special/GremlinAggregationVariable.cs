using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinAggregationVariable : GremlinVariable2, ISqlScalar
    {

    }

    internal class GremlinCountVariable : GremlinAggregationVariable
    {
        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }

    internal class GremlinFoldVariable : GremlinAggregationVariable
    {

    }

    internal class GremlinUnfoldVariable : GremlinTableVariable, ISqlTable
    {

    }
}
