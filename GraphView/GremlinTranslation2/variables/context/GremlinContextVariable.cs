using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVariable: GremlinVariable
    {
        public static GremlinContextVariable Create(GremlinVariable contextVariable)
        {
            if (contextVariable is GremlinContextVariable)
            {
                contextVariable = (contextVariable as GremlinContextVariable).ContextVariable;
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

        public GremlinVariable ContextVariable { get; set; }
        public bool IsFromSelect { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }
        public string SelectKey { get; set; }
        public List<string> UsedProperties { get; set; }

        public GremlinContextVariable(GremlinVariable contextVariable)
        {
            ContextVariable = contextVariable;
            VariableName = contextVariable.VariableName;
            UsedProperties = new List<string>();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return ContextVariable.GetVariableType();
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return ContextVariable.DefaultProjection();
        }

        internal override GremlinVariableProperty GetVariableProperty(string property)
        {
            return ContextVariable.GetVariableProperty(property);
        }

        internal override void Populate(string property)
        {
            ContextVariable.Populate(property);
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
            return ContextVariable.BottomUpPopulate(property, terminateVariable, alias, columnName);
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
                currentContext.SetPivotVariable(newVariableProperty);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        internal override void Select(GremlinToSqlContext currentContext, List<string> Labels)
        {
            ContextVariable.Select(currentContext, Labels);
        }

        internal override void Select(GremlinToSqlContext currentContext, string selectKey)
        {
            ContextVariable.Select(currentContext, selectKey);
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
            ContextVariable.Drop(currentContext);
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            ContextVariable.Property(currentContext, properties);
        }
    }

    internal class GremlinContextEdgeVariable : GremlinContextTableVariable
    {
        public GremlinContextEdgeVariable(GremlinVariable contextEdge) : base(contextEdge) { }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            ContextVariable.Property(currentContext, properties);
        }
    }
}
