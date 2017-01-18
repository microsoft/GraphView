using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVariable: GremlinSelectedVariable
    {
        public static GremlinContextVariable Create(GremlinVariable contextVariable)
        {
            //if (contextVariable is GremlinContextVariable)
            //{
            //    contextVariable = (contextVariable as GremlinContextVariable).ContextVariable;
            //}
            switch (contextVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinContextVertexVariable(contextVariable);
                case GremlinVariableType.Edge:
                    return new GremlinContextEdgeVariable(contextVariable);
                case GremlinVariableType.Table:
                    return new GremlinContextTableVariable(contextVariable);
                case GremlinVariableType.Scalar:
                    return new GremlinContextScalarVariable(contextVariable);

            }
            throw new NotImplementedException();
        }

        internal override string GetVariableName()
        {
            return RealVariable.GetVariableName();
        }


        public GremlinContextVariable(GremlinVariable contextVariable)
        {
            RealVariable = contextVariable;
            VariableName = contextVariable.VariableName;
            UsedProperties = new List<string>();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override GremlinVariableProperty GetVariableProperty(string property)
        {
            return RealVariable.GetVariableProperty(property);
        }

        internal override void Populate(string property)
        {
            RealVariable.Populate(property);
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
        }

        internal override string BottomUpPopulate(string property, GremlinVariable terminateVariable, string alias,
            string columnName = null)
        {
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
            return RealVariable.BottomUpPopulate(property, terminateVariable, alias, columnName);
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentContext, properties);
        }

        internal override void Select(GremlinToSqlContext currentContext, List<string> Labels)
        {
            RealVariable.Select(currentContext, Labels);
        }

        internal override void Select(GremlinToSqlContext currentContext, string selectKey)
        {
            RealVariable.Select(currentContext, selectKey);
        }
    }

    internal class GremlinContextScalarVariable : GremlinContextVariable
    {
        public GremlinContextScalarVariable(GremlinVariable contextVariable) : base(contextVariable) { }
    }

    internal class GremlinContextVertexVariable : GremlinContextTableVariable
    {
        public GremlinContextVertexVariable(GremlinVariable contextVariable) : base(contextVariable) { }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            RealVariable.Drop(currentContext);
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentContext, properties);
        }
    }

    internal class GremlinContextEdgeVariable : GremlinContextTableVariable
    {
        public GremlinContextEdgeVariable(GremlinVariable contextEdge) : base(contextEdge) { }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            RealVariable.Property(currentContext, properties);
        }
    }
}
