using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinAggregationVariable : GremlinVariable2, ISqlScalar
    {
        public WSelectElement ToSelectElement()
        {
            throw new NotImplementedException();
        }

        public WScalarExpression ToScalarExpression()
        {
            throw new NotImplementedException();
        }
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
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "UnFold_" + _count++;
        }

        public List<WSelectElement> ToSelectElementList()
        {
            return null;
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }
}
