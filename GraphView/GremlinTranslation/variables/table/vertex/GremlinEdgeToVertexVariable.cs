using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinEdgeToVertexVariable : GremlinVertexTableVariable
    {
        public GremlinVariable EdgeVariable { get; set; }

        public GremlinEdgeToVertexVariable(GremlinVariable edgeVariable)
        {
            GremlinVariableType inputVariableType = edgeVariable.GetVariableType();
            if (!(inputVariableType <= GremlinVariableType.Unknown && inputVariableType != GremlinVariableType.Vertex))
            {
                throw new SyntaxErrorException("The inputVariable of VertexToEdgeVariable must be a Edge");
            }

            this.EdgeVariable = edgeVariable;
        }
    }

    internal class GremlinEdgeToSourceVertexVariable : GremlinEdgeToVertexVariable
    {
        public GremlinEdgeToSourceVertexVariable(GremlinVariable edgeVariable) : base (edgeVariable) {}
        
        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.EdgeVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.EdgeToSourceVertex, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinEdgeToSinkVertexVariable : GremlinEdgeToVertexVariable
    {
        public GremlinEdgeToSinkVertexVariable(GremlinVariable edgeVariable) : base(edgeVariable) {}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.EdgeVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.EdgeToSinkVertex, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinEdgeToOtherVertexVariable : GremlinEdgeToVertexVariable
    {
        public GremlinEdgeToOtherVertexVariable(GremlinVariable edgeVariable) : base(edgeVariable) {}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.EdgeVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.EdgeToOtherVertex, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinEdgeToBothVertexVariable : GremlinEdgeToVertexVariable
    {
        public GremlinEdgeToBothVertexVariable(GremlinVariable edgeVariable) : base(edgeVariable) {}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.EdgeVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.EdgeToBothVertex, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
