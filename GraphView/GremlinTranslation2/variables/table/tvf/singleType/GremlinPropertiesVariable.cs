using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertiesVariable: GremlinPropertyTableVariable
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
            if (PropertyKeys.Count == 0)
            {
                parameters.Add(SqlUtil.GetColumnReferenceExpr(ProjectVariable.VariableName, "*"));
            }
            else
            {
                foreach (var property in PropertyKeys)
                {
                    parameters.Add(SqlUtil.GetColumnReferenceExpr(ProjectVariable.VariableName, property));
                }
            }
            
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Properties, parameters, this, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            currentContext.Key(this);
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            currentContext.Value(this);
        }
    }
}
