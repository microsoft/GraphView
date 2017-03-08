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

        /// <summary>
        /// g.addV("name", "marko", "name", "mike").property("name", "peter")
        /// => {"name": {cardinality: cardinality.single, label: "peter"}  marko and mike will be covered
        /// </summary>
        public Dictionary<string, List<GremlinProperty>> PropertyFromAddVParameters { get; set; }

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

            PropertyFromAddVParameters = new Dictionary<string, List<GremlinProperty>>();
            foreach (var property in vertexProperties)
            {
                ProjectedProperties.Add(property.Key);
                if (PropertyFromAddVParameters.ContainsKey(property.Key))
                {
                    PropertyFromAddVParameters[property.Key].Add(property);
                }
                else
                {
                    PropertyFromAddVParameters[property.Key] = new List<GremlinProperty> {property};
                }
            }
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty vertexProperty)
        {
            vertexProperty.Cardinality = GremlinKeyword.PropertyCardinality.list;
            if (PropertyFromAddVParameters.ContainsKey(vertexProperty.Key))
            {
                foreach (var property in PropertyFromAddVParameters[vertexProperty.Key])
                {
                    VertexProperties.Remove(property);
                }
            }
            ProjectedProperties.Add(vertexProperty.Key);
            VertexProperties.Add(vertexProperty);
        }
    }
}
