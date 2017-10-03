using System;
using System.Collections.Generic;
using System.Linq;

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
            if (ProjectedProperties.Contains(property))
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
                case GremlinVariableType.Scalar:
                case GremlinVariableType.Property:
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

    internal abstract class GremlinScalarTableVariable : GremlinTableVariable
    {
        protected GremlinScalarTableVariable(): base(GremlinVariableType.Scalar) {}
    }

    internal abstract class GremlinVertexTableVariable : GremlinTableVariable
    {
        protected GremlinVertexTableVariable(): base(GremlinVariableType.Vertex) {}
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        public WEdgeType EdgeType { get; set; }

        protected GremlinEdgeTableVariable(): base(GremlinVariableType.Edge) {}
    }
}
