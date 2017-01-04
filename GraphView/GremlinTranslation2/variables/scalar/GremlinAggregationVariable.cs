using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinAggregationVariable : GremlinScalarVariable
    {
        public GremlinScalarVariable AggregateProjection;

        public GremlinAggregationVariable(GremlinScalarVariable aggregateProjection)
        {
            AggregateProjection = aggregateProjection;
        }
    }

    internal class GremlinCountVariable : GremlinAggregationVariable
    {
        public GremlinCountVariable(GremlinScalarVariable aggregateProjection):base(aggregateProjection) {}

        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression()
            {
                SelectExpr = GremlinUtil.GetFunctionCall("count", GremlinUtil.GetStarColumnReferenceExpression())
            };
        }
    }

    internal class GremlinFoldVariable : GremlinAggregationVariable
    {
        public GremlinFoldVariable(GremlinScalarVariable aggregateProjection) : base(aggregateProjection) {}

        internal override void Unfold(ref GremlinToSqlContext currentContext) {}

        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression()
            {
                SelectExpr = GremlinUtil.GetFunctionCall("fold", AggregateProjection.ToScalarExpression())
            };
        }
    }

    internal class GremlinTreeVariable : GremlinScalarVariable
    {
        
    }

    internal class GremlinUnfoldVariable : GremlinTableVariable, ISqlTable
    {
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "UnFold_" + _count++;
        }

        public override  WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }

    
}
