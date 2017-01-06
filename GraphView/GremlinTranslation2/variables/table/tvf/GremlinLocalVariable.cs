using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalVariable : GremlinTableVariable
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
                    throw new NotImplementedException();
                case GremlinVariableType.Table:
                    throw new NotImplementedException();
            }
            throw new NotImplementedException();
        }

        public GremlinLocalVariable(GremlinToSqlContext localContext)
        {
            LocalContext = localContext;
            VariableName = GenerateTableAlias();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(SqlUtil.GetScalarSubquery(LocalContext.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = SqlUtil.GetFunctionTableReference("local", PropertyKeys, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinLocalVertexVariable : GremlinVertexTableVariable
    {
        public GremlinLocalVertexVariable(GremlinToSqlContext localContext)
        {
            InnerVariable = new GremlinLocalVariable(localContext);
        }
    }

    internal class GremlinLocalEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinLocalEdgeVariable(GremlinToSqlContext localContext)
        {
            InnerVariable = new GremlinLocalVariable(localContext);
        }
    }
}
