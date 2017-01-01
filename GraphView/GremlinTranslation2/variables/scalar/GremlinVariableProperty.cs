using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVariableProperty : GremlinScalarVariable
    {
        public GremlinVariable2 GremlinVariable { get; private set; }
        public string VariableProperty { get; private set; }

        public GremlinVariableProperty(GremlinVariable2 gremlinVariable, string variableProperty)
        {
            GremlinVariable = gremlinVariable;
            VariableProperty = variableProperty;
        }
        internal override GremlinScalarVariable DefaultProjection()
        {
            return this;
        }

        public override WSelectElement ToSelectElement()
        {
            return new WSelectScalarExpression()
            {
                SelectExpr = GremlinUtil.GetColumnReferenceExpression(GremlinVariable.VariableName, VariableProperty)
            };
        }

        public override WScalarExpression ToScalarExpression()
        {
            return GremlinUtil.GetColumnReferenceExpression(GremlinVariable.VariableName, VariableProperty);
        }
    }
}
