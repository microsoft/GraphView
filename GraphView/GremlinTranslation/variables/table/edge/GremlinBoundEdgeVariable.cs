using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundEdgeTableVariable : GremlinEdgeTableVariable
    {
        private GremlinVariableProperty sourceVertexVariableProperty;
        private GremlinVariableProperty adjEdgeVariableProperty;
        private GremlinVariableProperty revAdjEdgeVariableProperty;
        private GremlinVariableProperty labelVariableProperty;


        public GremlinBoundEdgeTableVariable(GremlinVariableProperty sourceVertexVariableProperty,
                                        GremlinVariableProperty adjEdgeVariableProperty,
                                        GremlinVariableProperty labelVariableProperty,
                                        WEdgeType edgeType)
        {
            this.sourceVertexVariableProperty = sourceVertexVariableProperty;
            this.adjEdgeVariableProperty = adjEdgeVariableProperty;
            this.labelVariableProperty = labelVariableProperty;
            EdgeType = edgeType;
        }

        public GremlinBoundEdgeTableVariable(GremlinVariableProperty sourceVertexVariableProperty,
                                        GremlinVariableProperty adjEdgeVariableProperty,
                                        GremlinVariableProperty revAdjEdgeVariableProperty,
                                        GremlinVariableProperty labelVariableProperty,
                                        WEdgeType edgeType)
        {
            this.sourceVertexVariableProperty = sourceVertexVariableProperty;
            this.adjEdgeVariableProperty = adjEdgeVariableProperty;
            this.revAdjEdgeVariableProperty = revAdjEdgeVariableProperty;
            this.labelVariableProperty = labelVariableProperty;
            EdgeType = edgeType;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            if (this.sourceVertexVariableProperty != null) PropertyKeys.Add(this.sourceVertexVariableProperty.ToScalarExpression());
            if (this.adjEdgeVariableProperty != null) PropertyKeys.Add(this.adjEdgeVariableProperty.ToScalarExpression());
            if (this.revAdjEdgeVariableProperty != null) PropertyKeys.Add(this.revAdjEdgeVariableProperty.ToScalarExpression());
            if (this.labelVariableProperty != null) PropertyKeys.Add(this.labelVariableProperty.ToScalarExpression());

            foreach (var property in ProjectedProperties)
            {
                PropertyKeys.Add(SqlUtil.GetValueExpr(property));
            }

            WTableReference tableRef = null;
            switch (this.EdgeType)
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
