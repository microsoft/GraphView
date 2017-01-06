using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapVariable: GremlinTableVariable
    {
        public GremlinToSqlContext FlatMapContext { get; set; }

        public static GremlinTableVariable Create(GremlinToSqlContext flatMapContext)
        {
            switch (flatMapContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinFlatMapVertexVariable(flatMapContext);
                case GremlinVariableType.Edge:
                    return new GremlinFlatMapEdgeVariable(flatMapContext);
                case GremlinVariableType.Scalar:
                    throw new NotImplementedException();
                case GremlinVariableType.Table:
                    throw new NotImplementedException();
            }
            throw new NotImplementedException();
        }

        public GremlinFlatMapVariable(GremlinToSqlContext flatMapContext)
        {
            VariableName = GenerateTableAlias();
            FlatMapContext = flatMapContext;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(SqlUtil.GetScalarSubquery(FlatMapContext.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference("flatMap", PropertyKeys, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinFlatMapVertexVariable : GremlinVertexTableVariable
    {
        public GremlinFlatMapVertexVariable(GremlinToSqlContext flatMapContext)
        {
            InnerVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }

    internal class GremlinFlatMapEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinFlatMapEdgeVariable(GremlinToSqlContext flatMapContext)
        {
            InnerVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }
}
