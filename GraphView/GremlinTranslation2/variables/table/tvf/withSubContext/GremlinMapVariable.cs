using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMapVariable : GremlinSqlTableVariable
    {
        public GremlinToSqlContext MapContext { get; set; }

        public static GremlinTableVariable Create(GremlinToSqlContext MapContext)
        {
            switch (MapContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinMapVertexVariable(MapContext);
                case GremlinVariableType.Edge:
                    return new GremlinMapEdgeVariable(MapContext);
                case GremlinVariableType.Scalar:
                    return new GremlinMapScalarVariable(MapContext);
                case GremlinVariableType.Table:
                    return new GremlinMapTableVariable(MapContext);
            }
            throw new QueryCompilationException();
        }

        public GremlinMapVariable(GremlinToSqlContext mapContext)
        {
            MapContext = mapContext;
        }

        internal override void Populate(string property)
        {
            MapContext.Populate(property);
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(MapContext.ToSelectQueryBlock(projectProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Map, parameters, gremlinVariable, tableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinMapVertexVariable : GremlinVertexTableVariable
    {
        public GremlinMapVertexVariable(GremlinToSqlContext MapContext)
        {
            SqlTableVariable = new GremlinMapVariable(MapContext);
        }
    }

    internal class GremlinMapEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinMapEdgeVariable(GremlinToSqlContext MapContext)
        {
            SqlTableVariable = new GremlinMapVariable(MapContext);
        }
    }

    internal class GremlinMapScalarVariable : GremlinEdgeTableVariable
    {
        public GremlinMapScalarVariable(GremlinToSqlContext MapContext)
        {
            SqlTableVariable = new GremlinMapVariable(MapContext);
        }
    }

    internal class GremlinMapTableVariable : GremlinEdgeTableVariable
    {
        public GremlinMapTableVariable(GremlinToSqlContext MapContext)
        {
            SqlTableVariable = new GremlinMapVariable(MapContext);
        }
    }
}
