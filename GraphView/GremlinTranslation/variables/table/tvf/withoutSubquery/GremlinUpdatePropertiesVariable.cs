using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphView
{
    internal class GremlinUpdatePropertiesVariable : GremlinTableVariable
    {
        public List<GremlinProperty> PropertyList { get; set; }
        public GremlinVariable UpdateVariable { get; set; }

        public GremlinUpdatePropertiesVariable(GremlinVariable updateVariable, GremlinProperty property): base(GremlinVariableType.NULL)
        {
            UpdateVariable = updateVariable;
            PropertyList = new List<GremlinProperty> { property };
        }

        public GremlinUpdatePropertiesVariable(GremlinVariable vertexVariable, List<GremlinProperty> properties) : base(GremlinVariableType.NULL)
        {
            UpdateVariable = vertexVariable;
            PropertyList = properties;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(UpdateVariable.DefaultProjection().ToScalarExpression());
            foreach (var vertexProperty in PropertyList)
            {
                parameters.Add(vertexProperty.ToPropertyExpr());
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateProperties, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
