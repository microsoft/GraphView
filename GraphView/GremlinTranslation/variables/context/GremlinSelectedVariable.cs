using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView {
    internal class GremlinSelectedVariable: GremlinVariable
    {
        public bool IsFromSelect { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }
        public string SelectKey { get; set; }
        public GremlinVariable RealVariable { get; set; }

        internal override WEdgeType GetEdgeType()
        {
            return RealVariable.GetEdgeType();
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    return GetVariableProperty(GremlinKeyword.EdgeID);
                case GremlinVariableType.Scalar:
                    return GetVariableProperty(GremlinKeyword.ScalarValue);
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.NodeID);
                case GremlinVariableType.Property:
                    return GetVariableProperty(GremlinKeyword.PropertyValue);
            }
            return GetVariableProperty(GremlinKeyword.TableDefaultColumnName);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    return GetVariableProperty(GremlinKeyword.Star);
                case GremlinVariableType.Scalar:
                    return GetVariableProperty(GremlinKeyword.ScalarValue);
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.Star);
                case GremlinVariableType.Property:
                    return GetVariableProperty(GremlinKeyword.PropertyValue);
            }
            return GetVariableProperty(GremlinKeyword.TableDefaultColumnName);
        }

        internal override string GetPrimaryKey()
        {
            return RealVariable.GetPrimaryKey();
        }

        internal override string GetProjectKey()
        {
            return RealVariable.GetProjectKey();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return RealVariable.GetUnfoldVariableType();
        }
    }
}
