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
            ColumnReferenceMap = new Dictionary<Tuple<string, string>, string>();
            UsedProperties = new List<string>();
            SelectKey = label;
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
                case GremlinVariableType.Table:
                    return new GremlinGhostTableVariable(realVariable, attachedVariable, label);
                case GremlinVariableType.Scalar:
                    return new GremlinGhostScalarVariable(realVariable, attachedVariable, label);

            }
            throw new NotImplementedException();
        }

        public GremlinVariable AttachedVariable { get; set; }
        public Dictionary<Tuple<string, string>, string> ColumnReferenceMap { get; set; }

        internal override GremlinVariableProperty GetVariableProperty(string property)
        {
            if (!ProjectedProperties.Contains(property)) Populate(property);
            var column = ColumnReferenceMap[new Tuple<string, string>(RealVariable.VariableName, property)];
            return new GremlinVariableProperty(AttachedVariable, column);
        }

        internal override string GetVariableName()
        {
            return AttachedVariable.GetVariableName();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            var defaultColumn = RealVariable.DefaultVariableProperty().VariableProperty;
            Populate(defaultColumn);
            return GetVariableProperty(defaultColumn);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            var defaultColumn = RealVariable.DefaultProjection().VariableProperty;
            Populate(defaultColumn);
            if (AttachedVariable is GremlinRepeatVariable)
            {
                return new GremlinVariableProperty(AttachedVariable, SelectKey);
            }
            else
            {
                return GetVariableProperty(defaultColumn);

            }
        }

        internal override void Populate(string property)
        {
            RealVariable.Populate(property);
            if (!ColumnReferenceMap.ContainsKey(new Tuple<string, string>(RealVariable.VariableName, property)))
            {
                var column = RealVariable.BottomUpPopulate(property, AttachedVariable, SelectKey);
                ColumnReferenceMap[new Tuple<string, string>(RealVariable.VariableName, property)] = column;
            }

            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
        }

        internal override string BottomUpPopulate(string property, GremlinVariable terminateVariable, string alias, string columnName = null)
        {
            //if we want to bottomUp populate a ghost Variable, then there are two part we should populate
            Populate(property);
            //RealVariable.Populate(property);

            if (terminateVariable == this) return property;
            if (HomeContext == null) throw new Exception();
            if (columnName == null)
            {
                columnName = alias + "_" + property;
            }
            HomeContext.AddProjectVariablePropertiesList(GetVariableProperty(property), columnName);
            if (HomeContext.HomeVariable == null) throw new Exception();
            return HomeContext.HomeVariable.BottomUpPopulate(columnName, terminateVariable, alias, columnName);
        }

        internal override void Property(GremlinToSqlContext currentGhost, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentGhost, properties);
        }

        internal override void Values(GremlinToSqlContext currentGhost, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 1)
            {
                GremlinVariableProperty newVariableProperty = RealVariable.GetVariableProperty(propertyKeys.First());
                currentGhost.VariableList.Add(newVariableProperty);
                currentGhost.SetPivotVariable(newVariableProperty);
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }

    internal class GremlinGhostScalarVariable : GremlinGhostVariable
    {
        public GremlinGhostScalarVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label) 
            : base(ghostVariable, attachedVariable, label) { }
    }

    internal class GremlinGhostVertexVariable : GremlinGhostTableVariable
    {
        public GremlinGhostVertexVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label) { }

        internal override void Drop(GremlinToSqlContext currentGhost)
        {
            RealVariable.Drop(currentGhost);
        }

        internal override void Property(GremlinToSqlContext currentGhost, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentGhost, properties);
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
    }

    internal class GremlinGhostEdgeVariable : GremlinGhostTableVariable
    {
        public GremlinGhostEdgeVariable(GremlinVariable ghostEdge, GremlinVariable attachedVariable, string label)
            : base(ghostEdge, attachedVariable, label) { }

        //internal override void Property(GremlinToSqlContext currentGhost, Dictionary<string, object> properties)
        //{
        //    RealVariable.Property(currentGhost, properties);
        //}

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
    }
}
