using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    /// <summary>
    /// A free vertex variable is translated to a node table reference in 
    /// the FROM clause, whereas a bound vertex variable is translated into
    /// a table-valued function following a prior table-valued function producing vertex references. 
    /// </summary>
    internal class GremlinBoundVertexVariable : GremlinVertexTableVariable
    {
        public WEdgeType InputEdgeType { get; set; }
        public GremlinVariableProperty SourceVariableProperty { get; set; }
        public GremlinVariableProperty SinkVariableProperty { get; set; }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            if (SourceVariableProperty != null)
            {
                PropertyKeys.Add(SourceVariableProperty.ToScalarExpression());
            }
            if (SinkVariableProperty != null)
            {
                PropertyKeys.Add(SinkVariableProperty.ToScalarExpression());
            }
            foreach (var property in ProjectedProperties)
            {
                PropertyKeys.Add(SqlUtil.GetValueExpr(property));
            }

            WTableReference secondTableRef = null;
            if (SourceVariableProperty != null && SinkVariableProperty != null)
            {
                //BothV
                secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothV, PropertyKeys, this, GetVariableName());
            }
            switch (InputEdgeType)
            {
                case WEdgeType.BothEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OtherV, PropertyKeys, this, GetVariableName());
                    break;
                case WEdgeType.InEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutV, PropertyKeys, this, GetVariableName());
                    break;
                case WEdgeType.OutEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InV, PropertyKeys, this, GetVariableName());
                    break;
                case WEdgeType.Undefined:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutV, PropertyKeys, this, GetVariableName());
                    break;
            }

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundVertexVariable(WEdgeType inputEdgeType, GremlinVariableProperty sourceVariableProperty)
        {
            InputEdgeType = inputEdgeType;
            SourceVariableProperty = sourceVariableProperty;
        }

        public GremlinBoundVertexVariable(WEdgeType inputEdgeType, GremlinVariableProperty sourceVariableProperty, GremlinVariableProperty sinkVariableProperty)
        {
            InputEdgeType = inputEdgeType;
            SourceVariableProperty = sourceVariableProperty;
            SinkVariableProperty = sinkVariableProperty;
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

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropVertex(this);
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
