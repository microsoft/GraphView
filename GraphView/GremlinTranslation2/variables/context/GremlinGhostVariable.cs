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
            Populate(property);
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
            return GetVariableProperty(defaultColumn);
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

            if (terminateVariable == this) return property;
            if (ParentContext == null) throw new Exception();
            if (columnName == null)
            {
                columnName = alias + "_" + property;
            }
            ParentContext.AddProjectVariablePropertiesList(GetVariableProperty(property), columnName);
            if (ParentContext.ParentVariable == null) throw new Exception();
            return ParentContext.ParentVariable.BottomUpPopulate(columnName, terminateVariable, alias, columnName);
        }

        internal override void Property(GremlinToSqlContext currentGhost, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentGhost, properties);
        }

        internal override void Values(GremlinToSqlContext currentGhost, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 1)
            {
                Populate(propertyKeys.First());
                GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(RealVariable,
                    propertyKeys.First());
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
    }

    internal class GremlinGhostEdgeVariable : GremlinGhostTableVariable
    {
        public GremlinGhostEdgeVariable(GremlinVariable ghostEdge, GremlinVariable attachedVariable, string label)
            : base(ghostEdge, attachedVariable, label) { }

        internal override void Property(GremlinToSqlContext currentGhost, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentGhost, properties);
        }
    }
}
