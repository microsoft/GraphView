using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDropVertexVariable : GremlinTableVariable
    {
        public GremlinVertexTableVariable VertexVariable { get; set; }

        public GremlinDropVertexVariable(GremlinVertexTableVariable vertexVariable)
        {
            VertexVariable = vertexVariable;
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
        public GremlinVertexTableVariable SourceVariable;
        public GremlinEdgeTableVariable EdgeVariable;

        public GremlinDropEdgeVariable(GremlinVertexTableVariable sourceVariable, GremlinEdgeTableVariable edgeVariable)
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

    internal class GremlinDropPropertiesVariable: GremlinTableVariable
    {

        public GremlinDropPropertiesVariable(GremlinVariable inputVariable)
        {
        }

        public override WTableReference ToTableReference()
        {
            throw new NotImplementedException();
            //List<WScalarExpression> parameters = new List<WScalarExpression>();
            //parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            //var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.DropEdge, parameters, VariableName);
            //return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
