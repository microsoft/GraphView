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
        /// => {"name": {cardinality: cardinality.Single, label: "peter"}  marko and mike will be covered
        /// </summary>
        public Dictionary<string, List<GremlinProperty>> PropertyFromAddVParameters { get; set; }

        public GremlinAddVVariable(string vertexLabel, List<GremlinProperty> vertexProperties, bool isFirstTableReference = false)
        {
            this.VertexProperties = new List<GremlinProperty>(vertexProperties);
            this.VertexLabel = vertexLabel;
            this.IsFirstTableReference = isFirstTableReference;
            this.ProjectedProperties.Add(GremlinKeyword.Label);

            this.PropertyFromAddVParameters = new Dictionary<string, List<GremlinProperty>>();
            foreach (var property in vertexProperties)
            {
                this.ProjectedProperties.Add(property.Key);
                if (this.PropertyFromAddVParameters.ContainsKey(property.Key))
                {
                    this.PropertyFromAddVParameters[property.Key].Add(property);
                }
                else
                {
                    this.PropertyFromAddVParameters[property.Key] = new List<GremlinProperty> { property };
                }
            }
        }

        internal override bool Populate(string property, string label = null)
        {
            if (this.ProjectedProperties.Contains(property))
            {
                return true;
            }
            else
            {
                if (base.Populate(property, label))
                {
                    this.VertexProperties.Add(new GremlinProperty(GremlinKeyword.PropertyCardinality.List, property, null, null));
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(this.VertexLabel));
            this.VertexProperties.Add(new GremlinProperty(GremlinKeyword.PropertyCardinality.List, GremlinKeyword.Star, null, null));
            parameters.AddRange(this.VertexProperties.Select(property => property.ToPropertyExpr()));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddV, parameters, GetVariableName());
            var crossApplyTableRef = SqlUtil.GetCrossApplyTableReference(secondTableRef);
            crossApplyTableRef.FirstTableRef = this.IsFirstTableReference ? SqlUtil.GetDerivedTable(SqlUtil.GetSimpleSelectQueryBlock("1"), "_") : null;
            return crossApplyTableRef;
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty vertexProperty)
        {
            vertexProperty.Cardinality = GremlinKeyword.PropertyCardinality.List;
            if (this.PropertyFromAddVParameters.ContainsKey(vertexProperty.Key))
            {
                foreach (var property in this.PropertyFromAddVParameters[vertexProperty.Key])
                {
                    this.VertexProperties.Remove(property);
                }
            }
            this.ProjectedProperties.Add(vertexProperty.Key);
            this.VertexProperties.Add(vertexProperty);
        }
    }
}
