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
            VariableType = variableType;
            VariableName = GremlinUtil.GenerateTableAlias(VariableType);
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            switch (GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    if (GremlinUtil.IsEdgeProperty(property)) return;
                    break;
                case GremlinVariableType.Edge:
                    if (GremlinUtil.IsVertexProperty(property)) return;
                    break;
                case GremlinVariableType.VertexProperty:
                    if (GremlinUtil.IsVertexProperty(property) || GremlinUtil.IsEdgeProperty(property)) return;
                    break;
                case GremlinVariableType.Scalar:
                case GremlinVariableType.Property:
                    if (property != GremlinKeyword.TableDefaultColumnName) return;
                    break;
            }
            base.Populate(property);
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return VariableType;
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
