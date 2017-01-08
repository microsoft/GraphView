using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertiesVariable: GremlinScalarTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinVariable ProjectVariable { get; set; }

        public GremlinPropertiesVariable(GremlinVariable projectVariable, List<string> propertyKeys)
        {
            ProjectVariable = projectVariable;
            PropertyKeys = new List<string>(propertyKeys);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var property in PropertyKeys)
            {
                parameters.Add(SqlUtil.GetColumnReferenceExpr(ProjectVariable.VariableName, property));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference("properties", parameters, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            GremlinKeyVariable newVariable = new GremlinKeyVariable(new GremlinVariableProperty(this, "_value"));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            GremlinValueVariable newVariable = new GremlinValueVariable(new GremlinVariableProperty(this, "_value"));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }
    }
}
