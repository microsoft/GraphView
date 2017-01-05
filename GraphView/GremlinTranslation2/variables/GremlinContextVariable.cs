using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVariable: GremlinVariable2
    {
        public GremlinVariable2 ContextVariable;

        public bool IsFromSelect;
        public GremlinKeyword.Pop Pop;
        public string SelectKey;

        public static GremlinContextVariable Create(GremlinVariable2 contextVariable)
        {
            if (contextVariable is GremlinContextVariable)
            {
                return contextVariable as GremlinContextVariable;
            }
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

        public GremlinContextVariable(GremlinVariable2 contextVariable)
        {
            ContextVariable = contextVariable;
            VariableName = contextVariable.VariableName;
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return ContextVariable.DefaultProjection();
        }

        internal override void Populate(string property)
        {
            ContextVariable.Populate(property);
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            ContextVariable.Property(currentContext, properties);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 1)
            {
                Populate(propertyKeys.First());
                GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(ContextVariable, propertyKeys.First());
                currentContext.VariableList.Add(newVariableProperty);
                currentContext.PivotVariable = newVariableProperty;
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
