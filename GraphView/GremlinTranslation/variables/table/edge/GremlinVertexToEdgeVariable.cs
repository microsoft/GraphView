using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVertexToEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinVariable VertexVariable { get; set; }

        public GremlinVertexToEdgeVariable(GremlinVariable vertexVariable)
        {
            GremlinVariableType inputVariableType = vertexVariable.GetVariableType();
            if (!(inputVariableType <= GremlinVariableType.Unknown && inputVariableType != GremlinVariableType.Edge))
            {
                throw new SyntaxErrorException("The inputVariable of VertexToEdgeVariable must be a vertex");
            }

            this.VertexVariable = vertexVariable;
        }
    }

    internal class GremlinVertexToForwardEdgeVariable : GremlinVertexToEdgeVariable
    {
        public GremlinVertexToForwardEdgeVariable(GremlinVariable vertexVariable) : base(vertexVariable) {}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.VertexVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.VertexToForwardEdge, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinVertexToBackwardEdgeVariable : GremlinVertexToEdgeVariable
    {
        public GremlinVertexToBackwardEdgeVariable(GremlinVariable vertexVariable) : base(vertexVariable) {}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.VertexVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.VertexToBackwardEdge, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinVertexToBothEdgeVariable : GremlinVertexToEdgeVariable
    {
        public GremlinVertexToBothEdgeVariable(GremlinVariable vertexVariable):base(vertexVariable) {}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.VertexVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.VertexToBothEdge, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
