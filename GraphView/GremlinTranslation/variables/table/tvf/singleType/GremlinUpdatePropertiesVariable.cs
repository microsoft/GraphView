using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphView
{
    internal class GremlinUpdatePropertiesVariable: GremlinDropVariable
    {
        public Dictionary<string, object> UpdateProperties { get; set; }

        public GremlinUpdatePropertiesVariable(Dictionary<string, object> properties)
        {
            UpdateProperties = new Dictionary<string, object>(properties);
        }

        internal override void Property(GremlinToSqlContext currenct, Dictionary<string, object> properties)
        {
            foreach (var property in properties)
            {
                UpdateProperties[property.Key] = property.Value;
            }
        }
    }

    internal class GremlinUpdateVertexPropertiesVariable : GremlinUpdatePropertiesVariable
    {
        public GremlinVariable VertexVariable { get; set; }

        public GremlinUpdateVertexPropertiesVariable(GremlinVariable vertexVariable,
            Dictionary<string, object> properties) : base(properties)
        {
            VertexVariable = vertexVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            parameters.Add(VertexVariable.DefaultVariableProperty().ToScalarExpression());
            foreach (var property in UpdateProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateNodeProperties, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinUpdateEdgePropertiesVariable: GremlinUpdatePropertiesVariable
    {
        public GremlinVariable EdgeVariable { get; set; }

        public GremlinUpdateEdgePropertiesVariable(GremlinVariable edgeVariable,
                                                    Dictionary<string, object> properties)
            : base(properties)
        {
            EdgeVariable = edgeVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            parameters.Add(EdgeVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV).ToScalarExpression());
            parameters.Add(EdgeVariable.GetVariableProperty(GremlinKeyword.EdgeID).ToScalarExpression());

            foreach (var property in UpdateProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateEdgeProperties, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
