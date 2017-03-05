using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        public WEdgeType EdgeType { get; set; }
        public GremlinVariableType VariableType { get; set; }

        public GremlinTableVariable(GremlinVariableType variableType)
        {
            VariableType = variableType;
            variableName = GremlinUtil.GenerateTableAlias(VariableType);
        }

        internal override WEdgeType GetEdgeType()
        {
            return EdgeType;
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

        //internal override GremlinVariableProperty DefaultProjection()
        //{
        //    switch (GetVariableType())
        //    {
        //        case GremlinVariableType.Edge:
        //        case GremlinVariableType.Vertex:
        //            return GetVariableProperty(GremlinKeyword.Star);
        //        default:
        //            return GetVariableProperty(GremlinKeyword.TableDefaultColumnName);
        //    }
        //}

        //internal override string GetPrimaryKey()
        //{
        //    string primaryKey;
        //    switch (GetVariableType())
        //    {
        //        case GremlinVariableType.Edge:
        //            primaryKey =  GremlinKeyword.EdgeID;
        //            break;
        //        case GremlinVariableType.Vertex:
        //            primaryKey = GremlinKeyword.NodeID;
        //            break;
        //        default:
        //            primaryKey = GremlinKeyword.TableDefaultColumnName;
        //            break;
        //    }
        //    Populate(primaryKey);
        //    return primaryKey;
        //}

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
        public GremlinEdgeTableVariable(): base(GremlinVariableType.Edge) {}
    }

    internal abstract class GremlinPropertyTableVariable : GremlinTableVariable
    {
        public GremlinPropertyTableVariable(): base(GremlinVariableType.Property) { }
    }

    internal abstract class GremlinDropVariable : GremlinTableVariable
    {
        public GremlinDropVariable() : base(GremlinVariableType.NULL) {}

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.NULL;
        }
    }
}
