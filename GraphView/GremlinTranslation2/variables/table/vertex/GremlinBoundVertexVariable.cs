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
                secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothV, PropertyKeys, this, VariableName);
            }
            switch (InputEdgeType)
            {
                case WEdgeType.BothEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OtherV, PropertyKeys, this, VariableName);
                    break;
                case WEdgeType.InEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutV, PropertyKeys, this, VariableName);
                    break;
                case WEdgeType.OutEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InV, PropertyKeys, this, VariableName);
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
    }
}
