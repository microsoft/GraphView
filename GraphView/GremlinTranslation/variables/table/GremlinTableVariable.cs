using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        public WEdgeType EdgeType { get; set; }
        public GremlinVariableType VariableType { get; set; }

        public GremlinTableVariable(GremlinVariableType variableType)
        {
            SetVariableTypeAndGenerateName(variableType);
        }

        public GremlinTableVariable()
        {
            SetVariableTypeAndGenerateName(GremlinVariableType.Table);
        }

        internal override WEdgeType GetEdgeType()
        {
            return EdgeType;
        }

        public void SetVariableTypeAndGenerateName(GremlinVariableType variableType)
        {
            VariableType = variableType;
            _variableName = GremlinUtil.GenerateTableAlias(VariableType);
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            switch (GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    if (GremlinUtil.IsEdgeProperty(property) || property == GremlinKeyword.TableDefaultColumnName) return;
                    break;
                case GremlinVariableType.Edge:
                    if (GremlinUtil.IsVertexProperty(property) || property == GremlinKeyword.TableDefaultColumnName) return;
                    break;
                case GremlinVariableType.Scalar:
                    if (property != GremlinKeyword.ScalarValue || property == GremlinKeyword.TableDefaultColumnName) return;
                    break;
                case GremlinVariableType.Property:
                    if (property != GremlinKeyword.PropertyValue || property == GremlinKeyword.TableDefaultColumnName) return;
                    break;
            }
            base.Populate(property);
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    return GetVariableProperty(GremlinKeyword.EdgeID);
                case GremlinVariableType.Scalar:
                    return GetVariableProperty(GremlinKeyword.ScalarValue);
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.NodeID);
                case GremlinVariableType.Property:
                    return GetVariableProperty(GremlinKeyword.PropertyValue);
            }
            return GetVariableProperty(GremlinKeyword.TableDefaultColumnName);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    return GetVariableProperty(GremlinKeyword.Star);
                case GremlinVariableType.Scalar:
                    return GetVariableProperty(GremlinKeyword.ScalarValue);
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.Star);
                case GremlinVariableType.Property:
                    return GetVariableProperty(GremlinKeyword.PropertyValue);
            }
            return GetVariableProperty(GremlinKeyword.TableDefaultColumnName);
        }

        internal override string GetPrimaryKey()
        {
            var primaryKey = "*";
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    primaryKey =  GremlinKeyword.EdgeID;
                    break;
                case GremlinVariableType.Scalar:
                    primaryKey = GremlinKeyword.ScalarValue;
                    break;
                case GremlinVariableType.Vertex:
                    primaryKey = GremlinKeyword.NodeID;
                    break;
                case GremlinVariableType.Property:
                    primaryKey = GremlinKeyword.PropertyValue;
                    break;
                case GremlinVariableType.Table:
                    primaryKey = GremlinKeyword.TableDefaultColumnName;
                    break;
            }
            Populate(primaryKey);
            return primaryKey;
        }

        internal override string GetProjectKey()
        {
            var projectKey = "*";
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    projectKey = GremlinKeyword.Star;
                    break;
                case GremlinVariableType.Scalar:
                    projectKey = GremlinKeyword.ScalarValue;
                    break;
                case GremlinVariableType.Vertex:
                    projectKey = GremlinKeyword.Star;
                    break;
                case GremlinVariableType.Property:
                    projectKey = GremlinKeyword.PropertyValue;
                    break;
                case GremlinVariableType.Table:
                    projectKey = GremlinKeyword.TableDefaultColumnName;
                    break;
            }
            Populate(projectKey);
            return projectKey;
        }

        internal override GremlinVariableType GetVariableType()
        {
            return VariableType;
        }
    }

    internal abstract class GremlinScalarTableVariable : GremlinTableVariable
    {
        public GremlinScalarTableVariable(): base(GremlinVariableType.Scalar) {}
    }

    internal abstract class GremlinVertexTableVariable : GremlinTableVariable
    {
        public GremlinVertexTableVariable(): base(GremlinVariableType.Vertex) {}

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }
        internal override void Drop(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new NotImplementedException();
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new NotImplementedException();
        }
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        public GremlinEdgeTableVariable(): base(GremlinVariableType.Edge) {}

        internal override void InV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new NotImplementedException();
        }
    }

    internal abstract class GremlinPropertyTableVariable : GremlinTableVariable
    {
        public GremlinPropertyTableVariable(): base(GremlinVariableType.Property) { }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void HasKey(GremlinToSqlContext currentContext, List<string> values)
        {
            throw new NotImplementedException();
        }

        internal override void HasValue(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
        }
    }

    internal abstract class GremlinDropVariable : GremlinTableVariable
    {
        public GremlinDropVariable() : base(GremlinVariableType.NULL) {}

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.NULL;
        }
    }
}
