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
        public List<object> UpdateProperties { get; set; }

        public GremlinUpdatePropertiesVariable(List<object> properties)
        {
            UpdateProperties = new List<object>(properties);
        }

        internal override void Property(GremlinToSqlContext currenct, List<object> properties)
        {
            UpdateProperties.AddRange(properties);
        }
    }

    internal class GremlinUpdateVertexPropertiesVariable : GremlinUpdatePropertiesVariable
    {
        public GremlinVariable VertexVariable { get; set; }

        public GremlinUpdateVertexPropertiesVariable(GremlinVariable vertexVariable,
            List<object> properties) : base(properties)
        {
            VertexVariable = vertexVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            parameters.Add(VertexVariable.DefaultVariableProperty().ToScalarExpression());
            for (var i = 0; i < UpdateProperties.Count; i += 2)
            {
                parameters.Add(SqlUtil.GetValueExpr(UpdateProperties[i]));
                parameters.Add(SqlUtil.GetValueExpr(UpdateProperties[i+1]));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateNodeProperties, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinUpdateEdgePropertiesVariable: GremlinUpdatePropertiesVariable
    {
        public GremlinVariable EdgeVariable { get; set; }

        public GremlinUpdateEdgePropertiesVariable(GremlinVariable edgeVariable,
                                                    List<object> properties)
            : base(properties)
        {
            EdgeVariable = edgeVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            parameters.Add(EdgeVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV).ToScalarExpression());
            parameters.Add(EdgeVariable.GetVariableProperty(GremlinKeyword.EdgeID).ToScalarExpression());

            for (var i = 0; i < UpdateProperties.Count; i += 2)
            {
                parameters.Add(SqlUtil.GetValueExpr(UpdateProperties[i]));
                parameters.Add(SqlUtil.GetValueExpr(UpdateProperties[i + 1]));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateEdgeProperties, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
