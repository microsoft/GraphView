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
        public GremlinContextVariable UpdateVariable { get; set; }

        public GremlinUpdatePropertiesVariable(GremlinVariable updateVariable, GremlinProperty property): base(GremlinVariableType.NULL)
        {
            UpdateVariable = new GremlinContextVariable(updateVariable);
            PropertyList = new List<GremlinProperty> { property };
        }

        public GremlinUpdatePropertiesVariable(GremlinVariable vertexVariable, List<GremlinProperty> properties) : base(GremlinVariableType.NULL)
        {
            UpdateVariable = new GremlinContextVariable(vertexVariable);
            PropertyList = properties;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(UpdateVariable.FetchAllVars());
            return variableList;
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
