using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalVariable : GremlinSqlTableVariable
    {
        public GremlinToSqlContext LocalContext { get; set; }

        public static GremlinTableVariable Create(GremlinToSqlContext localContext)
        {
            switch (localContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinLocalVertexVariable(localContext);
                case GremlinVariableType.Edge:
                    return new GremlinLocalEdgeVariable(localContext);
                case GremlinVariableType.Scalar:
                    return new GremlinLocalEdgeScalarVariable(localContext);
                case GremlinVariableType.Table:
                    return new GremlinLocalTableVariable(localContext);
            }
            throw new QueryCompilationException();
        }

        public GremlinLocalVariable(GremlinToSqlContext localContext)
        {
            LocalContext = localContext;
        }

        internal override void Populate(string property)
        {
            LocalContext.Populate(property);
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(LocalContext.ToSelectQueryBlock(projectProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Local, parameters, gremlinVariable, tableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinLocalVertexVariable : GremlinVertexTableVariable
    {
        public GremlinLocalVertexVariable(GremlinToSqlContext localContext)
        {
            SqlTableVariable = new GremlinLocalVariable(localContext);
        }
    }

    internal class GremlinLocalEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinLocalEdgeVariable(GremlinToSqlContext localContext)
        {
            SqlTableVariable = new GremlinLocalVariable(localContext);
        }
    }

    internal class GremlinLocalEdgeScalarVariable : GremlinScalarTableVariable
    {
        public GremlinLocalEdgeScalarVariable(GremlinToSqlContext localContext)
        {
            SqlTableVariable = new GremlinLocalVariable(localContext);
        }
    }

    internal class GremlinLocalTableVariable : GremlinTableVariable
    {
        public GremlinLocalTableVariable(GremlinToSqlContext localContext)
        {
            SqlTableVariable = new GremlinLocalVariable(localContext);
        }
    }
}
