using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDropVertexVariable : GremlinTableVariable
    {
        public GremlinVariableProperty DropVetexVariable { get; set; }

        public GremlinDropVertexVariable(GremlinVariableProperty dropVetexVariable)
        {
            DropVetexVariable = dropVetexVariable;
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            return DropVetexVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(DropVetexVariable.ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.DropNode, parameters, this, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinDropEdgeVariable : GremlinTableVariable
    {
        public GremlinVariableProperty SourceVariable;
        public GremlinVariableProperty EdgeVariable;

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            return EdgeVariable;
        }

        public GremlinDropEdgeVariable(GremlinVariableProperty sourceVariable, GremlinVariableProperty edgeVariable)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SourceVariable.ToScalarExpression());
            parameters.Add(EdgeVariable.ToScalarExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.DropEdge, parameters, this, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
