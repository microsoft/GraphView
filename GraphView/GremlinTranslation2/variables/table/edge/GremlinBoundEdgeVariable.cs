using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinVariableProperty SourceVertexVariableProperty;
        public GremlinVariableProperty AdjEdgeVariableProperty;
        public GremlinVariableProperty RevAdjEdgeVariableProperty;
        public GremlinVariableProperty LabelVariableProperty;

        public GremlinBoundEdgeVariable(GremlinVariableProperty sourceVertexVariableProperty,
                                        GremlinVariableProperty adjEdgeVariableProperty,
                                        GremlinVariableProperty labelVariableProperty,
                                        WEdgeType edgeType)
        {
            SourceVertexVariableProperty = sourceVertexVariableProperty;
            AdjEdgeVariableProperty = adjEdgeVariableProperty;
            LabelVariableProperty = labelVariableProperty;
            EdgeType = edgeType;
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty sourceVertexVariableProperty,
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
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothE, PropertyKeys, this, VariableName);
                    break;
                case WEdgeType.InEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InE, PropertyKeys, this, VariableName);
                    break;
                case WEdgeType.OutEdge:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutE, PropertyKeys, this, VariableName);
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
    }
}
