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

            WTableReference tableRef = null;
            switch (EdgeType)
            {
                case WEdgeType.BothEdge:
                    tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothE, PropertyKeys, GetVariableName());
                    break;
                case WEdgeType.InEdge:
                    tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InE, PropertyKeys, GetVariableName());
                    break;
                case WEdgeType.OutEdge:
                    tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutE, PropertyKeys, GetVariableName());
                    break;
            }

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
