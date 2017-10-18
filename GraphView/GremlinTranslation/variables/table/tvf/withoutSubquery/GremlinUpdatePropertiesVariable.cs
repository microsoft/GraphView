using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphView
{
    internal class GremlinUpdatePropertiesVariable : GremlinNULLTableVariable
    {
        public List<GremlinProperty> PropertyList { get; set; }
        public GremlinVariable UpdateVariable { get; set; }

        public GremlinUpdatePropertiesVariable(GremlinVariable updateVariable, GremlinProperty property)
        {
            this.UpdateVariable = updateVariable;
            this.PropertyList = new List<GremlinProperty> { property };
        }

        public GremlinUpdatePropertiesVariable(GremlinVariable vertexVariable, List<GremlinProperty> properties)
        {
            this.UpdateVariable = vertexVariable;
            this.PropertyList = properties;
        }

        internal override bool Populate(string property, string label = null)
        {
            return false;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.UpdateVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.UpdateVariable.DefaultProjection().ToScalarExpression());
            parameters.AddRange(this.PropertyList.Select(vertexProperty => vertexProperty.ToPropertyExpr()));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateProperties, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
