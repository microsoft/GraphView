using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphView {
    internal class GremlinUpdatePropertiesVariable: GremlinTableVariable
    {
        public Dictionary<string, object> Properties { get; set; }

        public GremlinUpdatePropertiesVariable(Dictionary<string, object> properties)
        {
            Properties = new Dictionary<string, object>(properties);
        }

        internal override void Property(GremlinToSqlContext currenct, Dictionary<string, object> properties)
        {
            foreach (var property in properties)
            {
                Properties[property.Key] = property.Value;
            }
        }
    }

    internal class GremlinUpdateNodePropertiesVariable : GremlinUpdatePropertiesVariable
    {
        public GremlinTableVariable VertexVariable { get; set; }

        public GremlinUpdateNodePropertiesVariable(GremlinTableVariable vertexVariable,
            Dictionary<string, object> properties) : base(properties)
        {
            VertexVariable = vertexVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(VertexVariable.DefaultProjection().ToScalarExpression());
            foreach (var property in Properties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateNodeProperties, parameters, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinUpdateEdgePropertiesVariable: GremlinUpdatePropertiesVariable
    {
        public GremlinTableVariable SourceVariable { get; set; }
        public GremlinTableVariable EdgeVariable { get; set; }

        public GremlinUpdateEdgePropertiesVariable(GremlinTableVariable sourceVariable,
            GremlinTableVariable edgeVariable,
            Dictionary<string, object> properties) : base(properties)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SourceVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(EdgeVariable.DefaultProjection().ToScalarExpression());
            foreach (var property in Properties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.UpdateEdgeProperties, parameters, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
