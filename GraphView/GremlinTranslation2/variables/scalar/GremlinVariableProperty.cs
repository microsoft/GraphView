using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVariableProperty : GremlinScalarVariable
    {
        public GremlinVariable GremlinVariable { get; set; }
        public string VariableProperty { get; set; }

        public GremlinVariableProperty(GremlinVariable gremlinVariable, string variableProperty)
        {
            GremlinVariable = gremlinVariable;
            VariableProperty = variableProperty;
        }

        public override WScalarExpression ToScalarExpression()
        {
            return SqlUtil.GetColumnReferenceExpr(GremlinVariable.VariableName, VariableProperty);
        }
    }
}
