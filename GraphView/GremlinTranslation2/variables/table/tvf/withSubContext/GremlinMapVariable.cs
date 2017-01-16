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

        internal override bool ContainsLabel(string label)
        {
            return false;
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
        public GremlinMapVertexVariable(GremlinToSqlContext mapContext)
        {
            SqlTableVariable = new GremlinMapVariable(mapContext);
        }
    }

    internal class GremlinMapEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinMapEdgeVariable(GremlinToSqlContext mapContext)
        {
            SqlTableVariable = new GremlinMapVariable(mapContext);
        }
    }

    internal class GremlinMapScalarVariable : GremlinScalarTableVariable
    {
        public GremlinMapScalarVariable(GremlinToSqlContext mapContext)
        {
            SqlTableVariable = new GremlinMapVariable(mapContext);
        }
    }

    internal class GremlinMapTableVariable : GremlinTableVariable
    {
        public GremlinMapTableVariable(GremlinToSqlContext mapContext)
        {
            SqlTableVariable = new GremlinMapVariable(mapContext);
        }
    }
}
