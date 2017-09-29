using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        public GremlinVariableType VariableType { get; set; }

        protected GremlinTableVariable(GremlinVariableType variableType)
        {
            this.VariableType = variableType;
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
                    if (property != GremlinKeyword.TableDefaultColumnName)
                    {
                        return false;
                    }
                    break;
            }
            return base.Populate(property, label);
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return this.VariableType;
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
