using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinVariableProperty : GremlinVariable
    {
        public GremlinVariable GremlinVariable { get; set; }
        public string VariableProperty { get; set; }

        public GremlinVariableProperty(GremlinVariable gremlinVariable, string variableProperty): base(GremlinVariableType.Property)
        {
            this.GremlinVariable = gremlinVariable;
            this.VariableProperty = variableProperty;
        }

        internal override bool Populate(string property, string label = null)
        {
            // no label
            bool populateSuccessfully = false;
            if (this.GremlinVariable.Populate(property, label))
            {
                populateSuccessfully = true;
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        public WScalarExpression ToScalarExpression()
        {
            return SqlUtil.GetColumnReferenceExpr(this.GremlinVariable.GetVariableName(), this.VariableProperty);
        }
    }
}
