using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGhostVariable : GremlinSelectedVariable
    {
        public GremlinGhostVariable(GremlinVariable realVariable, GremlinVariable attachedVariable, string label)
        {
            RealVariable = realVariable;
            AttachedVariable = attachedVariable;
            SelectKey = label;

            PropertiesMap = new Dictionary<string, string>();
        }

        public static GremlinGhostVariable Create(GremlinVariable realVariable, GremlinVariable attachedVariable, string label)
        {
            if (realVariable is GremlinGhostVariable)
            {
                return realVariable as GremlinGhostVariable;
            }
            switch (realVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinGhostVertexVariable(realVariable, attachedVariable, label);
                case GremlinVariableType.Edge:
                    return new GremlinGhostEdgeVariable(realVariable, attachedVariable, label);
                case GremlinVariableType.Scalar:
                    return new GremlinGhostScalarVariable(realVariable, attachedVariable, label);
                case GremlinVariableType.Property:
                    return new GremlinGhostPropertyVariable(realVariable, attachedVariable, label);

            }
            return new GremlinGhostVariable(realVariable, attachedVariable, label);
        }

        public GremlinVariable AttachedVariable { get; set; }
        public Dictionary<string, string> PropertiesMap { get; set; }

        internal override GremlinVariableProperty GetVariableProperty(string property)
        {
            Populate(property);
            return new GremlinVariableProperty(AttachedVariable, PropertiesMap[property]);
        }

        internal override string GetVariableName()
        {
            return AttachedVariable.GetVariableName();
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            if (RealVariable is GremlinRepeatSelectedVariable)
            {
                return new GremlinVariableProperty(AttachedVariable, SelectKey);
            }
            return GetVariableProperty(GetPrimaryKey());
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            if (RealVariable is GremlinRepeatSelectedVariable)
            {
                return new GremlinVariableProperty(AttachedVariable, SelectKey);
            }
            var defaultColumn = RealVariable.DefaultProjection().VariableProperty;
            return GetVariableProperty(defaultColumn);
        }

        internal override void Populate(string property)
        {
            RealVariable.Populate(property);

            if (ProjectedProperties.Contains(property)) return;
            switch (GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    if (GremlinUtil.IsEdgeProperty(property)) return;
                    break;
                case GremlinVariableType.Edge:
                    if (GremlinUtil.IsVertexProperty(property)) return;
                    break;
                case GremlinVariableType.Scalar:
                    if (property != GremlinKeyword.ScalarValue) return;
                    break;
                case GremlinVariableType.Property:
                    if (property != GremlinKeyword.PropertyValue) return;
                    break;
            }
            base.Populate(property);

            if (!PropertiesMap.ContainsKey(property))
            {
                string columnName = SelectKey + "_" + property;
                RealVariable.BottomUpPopulate(AttachedVariable, property, columnName);
                PropertiesMap[property] = columnName;
            }
        }
    }

    internal class GremlinGhostVertexVariable : GremlinGhostVariable
    {
        public GremlinGhostVertexVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label) { }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentContext, properties);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Both(this, edgeLabels);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.BothE(this, edgeLabels);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            currentContext.BothV(this);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.In(this, edgeLabels);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.InE(this, edgeLabels);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Out(this, edgeLabels);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.OutE(this, edgeLabels);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropVertex(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }

    internal class GremlinGhostEdgeVariable : GremlinGhostVariable
    {
        public GremlinGhostEdgeVariable(GremlinVariable ghostEdge, GremlinVariable attachedVariable, string label)
            : base(ghostEdge, attachedVariable, label) { }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.InV(this);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.OutV(this);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            currentContext.OtherV(this);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropEdge(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }

    internal class GremlinGhostScalarVariable : GremlinGhostVariable
    {
        public GremlinGhostScalarVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label) { }
    }

    internal class GremlinGhostPropertyVariable : GremlinGhostVariable
    {
        public GremlinGhostPropertyVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label)
        { }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            currentContext.Key(this);
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            currentContext.Value(this);
        }
    }

    internal class GremlinGhostTableVariable : GremlinGhostVariable
    {
        public GremlinGhostTableVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label) { }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.InV(this);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.OutV(this);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            currentContext.OtherV(this);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Both(this, edgeLabels);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.BothE(this, edgeLabels);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            currentContext.BothV(this);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.In(this, edgeLabels);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.InE(this, edgeLabels);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Out(this, edgeLabels);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.OutE(this, edgeLabels);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropVertex(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }
}
