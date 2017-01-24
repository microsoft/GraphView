using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundEdgeVariable : GremlinEdgeTableVariable
    {
        private List<GremlinVariableProperty> variablePropertyList;

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            foreach (var variableProperty in variablePropertyList)
            {
                PropertyKeys.Add(variableProperty.ToScalarExpression());
            }
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

        public GremlinBoundEdgeVariable(GremlinVariableProperty sourceProperty, GremlinVariableProperty adjEdge, GremlinVariableProperty labelProperty, WEdgeType edgeType)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(sourceProperty);
            variablePropertyList.Add(adjEdge);
            variablePropertyList.Add(labelProperty);
            EdgeType = edgeType;
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty sourceProperty, GremlinVariableProperty adjEdge, GremlinVariableProperty adjReverseEdge, GremlinVariableProperty labelProperty, WEdgeType edgeType)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(sourceProperty);
            variablePropertyList.Add(adjEdge);
            variablePropertyList.Add(adjReverseEdge);
            variablePropertyList.Add(labelProperty);
            EdgeType = edgeType;
        }
    }
}
