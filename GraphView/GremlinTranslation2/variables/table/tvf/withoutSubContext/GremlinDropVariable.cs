using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDropVertexVariable : GremlinTableVariable
    {
        public GremlinTableVariable VertexVariable { get; set; }

        public GremlinDropVertexVariable(GremlinTableVariable vertexVariable)
        {
            VertexVariable = vertexVariable;
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return VertexVariable.DefaultProjection();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(VertexVariable.DefaultProjection().ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.DropNode, parameters, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinDropEdgeVariable : GremlinTableVariable
    {
        public GremlinTableVariable SourceVariable;
        public GremlinTableVariable EdgeVariable;

        internal override GremlinScalarVariable DefaultProjection()
        {
            return EdgeVariable.DefaultProjection();
        }

        public GremlinDropEdgeVariable(GremlinTableVariable sourceVariable, GremlinTableVariable edgeVariable)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SourceVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(EdgeVariable.DefaultProjection().ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.DropEdge, parameters, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
