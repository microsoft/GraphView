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
            return base.DefaultProjection();
            //return GetVariableProperty(GetPrimaryKey());
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
                case GremlinVariableType.Property:
                    if (property != GremlinKeyword.TableDefaultColumnName) return;
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

    }

    internal class GremlinGhostEdgeVariable : GremlinGhostVariable
    {
        public GremlinGhostEdgeVariable(GremlinVariable ghostEdge, GremlinVariable attachedVariable, string label)
            : base(ghostEdge, attachedVariable, label) { }
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
    }

    internal class GremlinGhostTableVariable : GremlinGhostVariable
    {
        public GremlinGhostTableVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label) { }
    }
}
