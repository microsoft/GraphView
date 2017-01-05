using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertiesVariable: GremlinTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable2 ProjectVariable { get; set; }

        public GremlinPropertiesVariable(GremlinVariable2 projectVariable, List<string> propertyKeys)
        {
            ProjectVariable = projectVariable;
            PropertyKeys = new List<string>(propertyKeys);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var property in PropertyKeys)
            {
                parameters.Add(GremlinUtil.GetColumnReferenceExpr(ProjectVariable.VariableName, property));
            }
            var secondTableRef = GremlinUtil.GetFunctionTableReference("properties", parameters, VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(this, "_key");
            currentContext.VariableList.Add(newVariableProperty);
            currentContext.PivotVariable = newVariableProperty;
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(this, "_value");
            currentContext.VariableList.Add(newVariableProperty);
            currentContext.PivotVariable = newVariableProperty;
        }
    }
}
