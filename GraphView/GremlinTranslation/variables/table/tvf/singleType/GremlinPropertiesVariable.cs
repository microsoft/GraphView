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
        public GremlinVariable InputVariable { get; set; }

        public GremlinPropertiesVariable(GremlinVariable inputVariable, List<string> propertyKeys)
            :base(GremlinVariableType.VertexProperty)
        {
            InputVariable = inputVariable;
            PropertyKeys = new List<string>(propertyKeys);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            bool isFetchAll = false;
            if (PropertyKeys.Count == 0)
            {
                parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
                isFetchAll = true;
            }
            else
            {
                foreach (var property in PropertyKeys)
                {
                    parameters.Add(InputVariable.GetVariableProperty(property).ToScalarExpression());
                }
            }

            foreach (var projectProperty in ProjectedProperties)
            {
                if (projectProperty == GremlinKeyword.TableDefaultColumnName) continue;
                parameters.Add(SqlUtil.GetValueExpr(projectProperty));
            }

            var tableRef = SqlUtil.GetFunctionTableReference(isFetchAll ? GremlinKeyword.func.AllProperties : GremlinKeyword.func.Properties,
                                                                    parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
