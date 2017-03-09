using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        public GremlinVariableType VariableType { get; set; }

        public GremlinTableVariable(GremlinVariableType variableType)
        {
            VariableType = variableType;
            variableName = GremlinUtil.GenerateTableAlias(VariableType);
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

        internal override string GetProjectKey()
        {
            string projectKey;
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                case GremlinVariableType.Vertex:
                    projectKey = GremlinKeyword.Star;
                    break;
                default:
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
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        public WEdgeType EdgeType { get; set; }

        public GremlinEdgeTableVariable(): base(GremlinVariableType.Edge) {}
    }
}
