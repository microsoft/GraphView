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
                    return new GremlinFlatMapScalarVariable(flatMapContext);
                case GremlinVariableType.Table:
                    return new GremlinFlatMapTableVariable(flatMapContext);
            }
            throw new QueryCompilationException();
        }

        public GremlinFlatMapVariable(GremlinToSqlContext flatMapContext)
        {
            FlatMapContext = flatMapContext;
        }

        internal override void Populate(string property)
        {
            FlatMapContext.Populate(property);
        }

        internal override bool ContainsLabel(string label)
        {
            return false;
        }
        internal override List<GremlinVariable> FetchAllVariablesInCurrAndChildContext()
        {
            return FlatMapContext.FetchAllVariablesInCurrAndChildContext();
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(FlatMapContext.ToSelectQueryBlock(projectProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.FlatMap, parameters, gremlinVariable, tableName);

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

    internal class GremlinFlatMapScalarVariable : GremlinScalarTableVariable
    {
        public GremlinFlatMapScalarVariable(GremlinToSqlContext flatMapContext)
        {
            SqlTableVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }

    internal class GremlinFlatMapTableVariable : GremlinTableVariable
    {
        public GremlinFlatMapTableVariable(GremlinToSqlContext flatMapContext)
        {
            SqlTableVariable = new GremlinFlatMapVariable(flatMapContext);
        }
    }
}
