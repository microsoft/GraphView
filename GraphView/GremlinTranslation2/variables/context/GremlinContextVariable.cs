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
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
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
            return base.BottomUpPopulate(property, terminateVariable, alias, columnName);
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

        internal override void Key(GremlinToSqlContext currentContext)
        {
            GremlinKeyVariable newVariable = new GremlinKeyVariable(RealVariable.DefaultVariableProperty());
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            GremlinValueVariable newVariable = new GremlinValueVariable(RealVariable.DefaultVariableProperty());
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }
    }

    internal class GremlinContextVertexVariable : GremlinContextTableVariable
    {
        public GremlinContextVertexVariable(GremlinVariable contextVariable) : base(contextVariable) { }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            RealVariable.Drop(currentContext);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Both(this, edgeLabels);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.BothE(this, edgeLabels);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            currentContext.BothV(this);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.In(this, edgeLabels);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.InE(this, edgeLabels);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Out(this, edgeLabels);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.OutE(this, edgeLabels);
        }
    }

    internal class GremlinContextEdgeVariable : GremlinContextTableVariable
    {
        public GremlinContextEdgeVariable(GremlinVariable contextEdge) : base(contextEdge) { }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.InV(this);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.OutV(this);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            currentContext.OtherV(this);
        }
    }
}
