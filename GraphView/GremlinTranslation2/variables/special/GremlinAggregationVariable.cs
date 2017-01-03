using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinAggregationVariable : GremlinScalarVariable, ISqlScalar
    {
        protected static int _count = 0;
        internal virtual string GenerateTableAlias()
        {
            return "Agg_" + _count++;
        }

        public GremlinScalarVariable AggregateProjection;
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
        public GremlinFoldVariable(GremlinScalarVariable aggregateProjection)
        {
            VariableName = GenerateTableAlias();
            AggregateProjection = aggregateProjection;
        }

        internal override void Unfold(ref GremlinToSqlContext currentContext)
        {
        }

        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression()
            {
                SelectExpr = GremlinUtil.GetFunctionCall("fold", AggregateProjection.ToScalarExpression())
            };
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return this;
        }
    }

    internal class GremlinUnfoldVariable : GremlinTableVariable, ISqlTable
    {
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "UnFold_" + _count++;
        }

        public WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }
    }
}
