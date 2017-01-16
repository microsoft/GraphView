using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGhostVariable : GremlinVariable
    {
        public GremlinGhostVariable(GremlinVariable realVariable, GremlinVariable attachedVariable)
        {
            RealVariable = realVariable;
            AttachedVariable = attachedVariable;
            ColumnReferenceMap = new Dictionary<Tuple<string, string>, Tuple<string, string>>();
            UsedProperties = new List<string>();
        }

        public static GremlinGhostVariable Create(GremlinVariable realVariable, GremlinVariable attachedVariable)
        {
            if (realVariable is GremlinGhostVariable)
            {
                return realVariable as GremlinGhostVariable;
            }
            switch (realVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinGhostVertexVariable(realVariable, attachedVariable);
                case GremlinVariableType.Edge:
                    return new GremlinGhostEdgeVariable(realVariable, attachedVariable);
                case GremlinVariableType.Table:
                    return new GremlinGhostTableVariable(realVariable, attachedVariable);
                case GremlinVariableType.Scalar:
                    return new GremlinGhostScalarVariable(realVariable, attachedVariable);

            }
            throw new NotImplementedException();
        }

        public GremlinVariable RealVariable { get; set; }
        public GremlinVariable AttachedVariable { get; set; }
        public bool IsFromSelect { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }
        public string SelectKey { get; set; }
        public List<string> UsedProperties { get; set; }
        public Dictionary<Tuple<string, string>, Tuple<string, string>> ColumnReferenceMap { get; set; }

        internal GremlinVariableProperty GetVariableProperty(string property)
        {
            var temp = ColumnReferenceMap[new Tuple<string, string>(RealVariable.VariableName, property)];
            return new GremlinVariableProperty(AttachedVariable, temp.Item2);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return RealVariable.DefaultProjection();
        }

        internal override void Populate(string property)
        {
            if (!ColumnReferenceMap.ContainsKey(new Tuple<string, string>(RealVariable.VariableName, property)))
            {
                var column = RealVariable.BottomUpPopulate(property, AttachedVariable, SelectKey);
                ColumnReferenceMap[new Tuple<string, string>(RealVariable.VariableName, property)] =
                    new Tuple<string, string>(AttachedVariable.VariableName, column);
            }

            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
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
        public GremlinGhostScalarVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable) : base(ghostVariable, attachedVariable) { }
    }

    internal class GremlinGhostVertexVariable : GremlinGhostTableVariable
    {
        public GremlinGhostVertexVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable) : base(ghostVariable, attachedVariable) { }

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
        public GremlinGhostEdgeVariable(GremlinVariable ghostEdge, GremlinVariable attachedVariable) : base(ghostEdge, attachedVariable) { }

        internal override void Property(GremlinToSqlContext currentGhost, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentGhost, properties);
        }
    }
}
