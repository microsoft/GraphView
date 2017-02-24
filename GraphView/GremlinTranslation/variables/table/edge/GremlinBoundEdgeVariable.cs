using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundEdgeTableVariable : GremlinEdgeTableVariable
    {
        public GremlinVariableProperty SourceVertexVariableProperty;
        public GremlinVariableProperty AdjEdgeVariableProperty;
        public GremlinVariableProperty RevAdjEdgeVariableProperty;
        public GremlinVariableProperty LabelVariableProperty;

        public GremlinBoundEdgeTableVariable(GremlinVariableProperty sourceVertexVariableProperty,
                                        GremlinVariableProperty adjEdgeVariableProperty,
                                        GremlinVariableProperty labelVariableProperty,
                                        WEdgeType edgeType)
        {
            SourceVertexVariableProperty = sourceVertexVariableProperty;
            AdjEdgeVariableProperty = adjEdgeVariableProperty;
            LabelVariableProperty = labelVariableProperty;
            EdgeType = edgeType;
        }

        public GremlinBoundEdgeTableVariable(GremlinVariableProperty sourceVertexVariableProperty,
                                        GremlinVariableProperty adjEdgeVariableProperty,
                                        GremlinVariableProperty revAdjEdgeVariableProperty,
                                        GremlinVariableProperty labelVariableProperty,
                                        WEdgeType edgeType)
        {
            SourceVertexVariableProperty = sourceVertexVariableProperty;
            AdjEdgeVariableProperty = adjEdgeVariableProperty;
            RevAdjEdgeVariableProperty = revAdjEdgeVariableProperty;
            LabelVariableProperty = labelVariableProperty;
            EdgeType = edgeType;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            if (SourceVertexVariableProperty != null) PropertyKeys.Add(SourceVertexVariableProperty.ToScalarExpression());
            if (AdjEdgeVariableProperty != null) PropertyKeys.Add(AdjEdgeVariableProperty.ToScalarExpression());
            if (RevAdjEdgeVariableProperty != null) PropertyKeys.Add(RevAdjEdgeVariableProperty.ToScalarExpression());
            if (LabelVariableProperty != null) PropertyKeys.Add(LabelVariableProperty.ToScalarExpression());

            foreach (var property in ProjectedProperties)
            {
                PropertyKeys.Add(SqlUtil.GetValueExpr(property));
            }

            WTableReference secondTableRef = null;
            switch (EdgeType)
            {
                case WEdgeType.BothEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothE, PropertyKeys, this, GetVariableName());
                    break;
                case WEdgeType.InEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InE, PropertyKeys, this, GetVariableName());
                    break;
                case WEdgeType.OutEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutE, PropertyKeys, this, GetVariableName());
                    break;
            }

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

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

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropEdge(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }
}
