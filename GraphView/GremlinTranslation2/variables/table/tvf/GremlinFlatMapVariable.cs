using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapVariable: GremlinSqlTableVariable
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
            FlatMapContext = flatMapContext;
        }

        internal override void Populate(string property)
        {
            FlatMapContext.Populate(property);
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName)
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(SqlUtil.GetScalarSubquery(FlatMapContext.ToSelectQueryBlock(projectProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference("flatMap", PropertyKeys, tableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinFlatMapVertexVariable : GremlinVertexTableVariable
    {
        public GremlinFlatMapVertexVariable(GremlinToSqlContext flatMapContext)
        {
            SqlTableVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }

    internal class GremlinFlatMapEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinFlatMapEdgeVariable(GremlinToSqlContext flatMapContext)
        {
            SqlTableVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }

    internal class GremlinFlatMapScalarVariable : GremlinEdgeTableVariable
    {
        public GremlinFlatMapScalarVariable(GremlinToSqlContext flatMapContext)
        {
            SqlTableVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }

    internal class GremlinFlatMapTableVariable : GremlinEdgeTableVariable
    {
        public GremlinFlatMapTableVariable(GremlinToSqlContext flatMapContext)
        {
            SqlTableVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }
}
