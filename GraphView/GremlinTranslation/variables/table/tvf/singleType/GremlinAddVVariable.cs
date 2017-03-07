using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddVVariable: GremlinVertexTableVariable
    {
        public List<GremlinProperty> VertexProperties { get; set; }
        public string VertexLabel { get; set; }
        public bool IsFirstTableReference { get; set; }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            VertexProperties.Add(new GremlinProperty(GremlinKeyword.PropertyCardinality.list, property, null, null));
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(VertexLabel));
            foreach (var vertexProperty in VertexProperties)
            {
                parameters.Add(vertexProperty.ToPropertyExpr());
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddV, parameters, GetVariableName());
            var crossApplyTableRef = SqlUtil.GetCrossApplyTableReference(secondTableRef);
            crossApplyTableRef.FirstTableRef = IsFirstTableReference ? SqlUtil.GetDerivedTable(SqlUtil.GetSimpleSelectQueryBlock("1"), "_") : null;
            return crossApplyTableRef;
        }

        public GremlinAddVVariable(string vertexLabel, List<GremlinProperty> vertexProperties, bool isFirstTableReference = false)
        {
            VertexProperties = new List<GremlinProperty>(vertexProperties);
            VertexLabel = vertexLabel;
            IsFirstTableReference = isFirstTableReference;
            ProjectedProperties.Add(GremlinKeyword.Label);
            foreach (var property in vertexProperties)
            {
                ProjectedProperties.Add(property.Key);
            }
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty vertexProperty)
        {
            vertexProperty.Cardinality = GremlinKeyword.PropertyCardinality.list;
            ProjectedProperties.Add(vertexProperty.Key);
            VertexProperties.Add(vertexProperty);
        }
    }
}
