using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVariableProperty : GremlinVariable
    {
        public GremlinContextVariable GremlinVariable { get; set; }
        public string VariableProperty { get; set; }

        public GremlinVariableProperty(GremlinVariable gremlinVariable, string variableProperty): base(GremlinVariableType.Scalar)
        {
            GremlinVariable = new GremlinContextVariable(gremlinVariable);
            VariableProperty = variableProperty;
        }

        internal override void Populate(string property)
        {
            GremlinVariable.Populate(property);
            base.Populate(property);
        }

        public WScalarExpression ToScalarExpression()
        {
            return SqlUtil.GetColumnReferenceExpr(GremlinVariable.GetVariableName(), VariableProperty);
        }
    }
}
