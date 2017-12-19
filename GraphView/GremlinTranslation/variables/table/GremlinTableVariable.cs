using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualBasic.ApplicationServices;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        protected GremlinTableVariable(GremlinVariableType variableType): base(variableType)
        {
            this.VariableName = GremlinUtil.GenerateTableAlias(this.VariableType);
        }

        internal override bool Populate(string property, string label = null)
        {
            if (property == null && label == null)
            {
                return true;
            }
            switch (this.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    if (GremlinUtil.IsEdgeProperty(property))
                    {
                        return false;
                    }
                    break;
                case GremlinVariableType.Edge:
                    if (GremlinUtil.IsVertexProperty(property))
                    {
                        return false;
                    }
                    break;
                case GremlinVariableType.VertexProperty:
                    if (GremlinUtil.IsVertexProperty(property) || GremlinUtil.IsEdgeProperty(property))
                    {
                        return false;
                    }
                    break;
                case GremlinVariableType.VertexAndEdge:
                case GremlinVariableType.Mixed:
                case GremlinVariableType.Unknown:
                case GremlinVariableType.NULL:
                    break;
                default:
                    return false;
            }
            return base.Populate(property, label);
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        internal virtual List<GremlinTableVariable> FetchAllTableVars()
        {
            return new List<GremlinTableVariable> { this };
        }
    }

    internal abstract class GremlinVertexTableVariable : GremlinTableVariable
    {
        protected GremlinVertexTableVariable(): base(GremlinVariableType.Vertex) { }
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        public WEdgeType EdgeType { get; set; }

        protected GremlinEdgeTableVariable(): base(GremlinVariableType.Edge) { }
    }

    internal abstract class GremlinScalarTableVariable : GremlinTableVariable
    {
        protected GremlinScalarTableVariable() : base(GremlinVariableType.Scalar) { }
    }

    internal abstract class GremlinVertexPropertyTableVariable : GremlinTableVariable
    {
        protected GremlinVertexPropertyTableVariable() : base(GremlinVariableType.VertexProperty) { }
    }
    
    internal abstract class GremlinNULLTableVariable : GremlinTableVariable
    {
        protected GremlinNULLTableVariable() : base(GremlinVariableType.NULL) { }
    }

    internal abstract class GremlinFilterTableVariable : GremlinTableVariable
    {
        protected GremlinFilterTableVariable(GremlinVariableType variableType) : base(variableType) { }
    }

    internal abstract class GremlinListTableVariable : GremlinTableVariable
    {
        protected GremlinListTableVariable() : base(GremlinVariableType.List) { }
    }

    internal abstract class GremlinMapTableVariable : GremlinTableVariable
    {
        protected GremlinMapTableVariable() : base(GremlinVariableType.Map) { }
    }
}
