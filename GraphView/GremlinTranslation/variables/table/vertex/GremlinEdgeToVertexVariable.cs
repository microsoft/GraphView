using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinEdgeToVertexVariable : GremlinVertexTableVariable
    {
        public GremlinContextVariable EdgeVariable { get; set; }

        public GremlinEdgeToVertexVariable(GremlinVariable edgeVariable)
        {
            this.EdgeVariable = new GremlinContextVariable(edgeVariable);
        }
    }

    internal class GremlinEdgeToSourceVertexVariable : GremlinEdgeToVertexVariable
    {
        public GremlinEdgeToSourceVertexVariable(GremlinVariable edgeVariable) :base (edgeVariable) {}
        
        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.EdgeVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(this.DefaultProperty()));
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
            parameters.Add(SqlUtil.GetValueExpr(this.DefaultProperty()));
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
            parameters.Add(SqlUtil.GetValueExpr(this.DefaultProperty()));
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
            parameters.Add(SqlUtil.GetValueExpr(this.DefaultProperty()));
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.EdgeToBothVertex, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
