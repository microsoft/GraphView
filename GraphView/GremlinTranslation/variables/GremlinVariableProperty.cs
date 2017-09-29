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

        public GremlinVariableProperty(GremlinVariable gremlinVariable, string variableProperty)
        {
            this.GremlinVariable = new GremlinContextVariable(gremlinVariable);
            this.VariableProperty = variableProperty;
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (this.GremlinVariable.Populate(property, label))
            {
                return base.Populate(property, null);
            }
            else
            {
                return false;
            }
        }

        public WScalarExpression ToScalarExpression()
        {
            return SqlUtil.GetColumnReferenceExpr(this.GremlinVariable.GetVariableName(), this.VariableProperty);
        }
    }
}
