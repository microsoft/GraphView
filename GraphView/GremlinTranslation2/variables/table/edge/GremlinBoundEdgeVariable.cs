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
            Populate(GremlinKeyword.EdgeID);
            foreach (var property in ProjectedProperties)
            {
                PropertyKeys.Add(SqlUtil.GetValueExpr(property));
            }
            WTableReference secondTableRef = null;
            switch (GremlinEdgeType)
            {
                case GremlinEdgeType.BothE:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothE, PropertyKeys, this, VariableName);
                    break;
                case GremlinEdgeType.BothForwardE:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothForwardE, PropertyKeys, this, VariableName);
                    break;
                case GremlinEdgeType.InE:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InE, PropertyKeys, this, VariableName);
                    break;
                case GremlinEdgeType.InForwardE:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InForwardE, PropertyKeys, this, VariableName);
                    break;
                case GremlinEdgeType.OutE:
                    secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutE, PropertyKeys, this, VariableName);
                    break;
            }

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty sourceProperty, GremlinVariableProperty adjReverseEdge, WEdgeType edgeType, GremlinEdgeType gremlinEdgeType)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(sourceProperty);
            variablePropertyList.Add(adjReverseEdge);
            EdgeType = edgeType;
            GremlinEdgeType = gremlinEdgeType;
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty sourceProperty, GremlinVariableProperty adjEdge, GremlinVariableProperty adjReverseEdge, WEdgeType edgeType, GremlinEdgeType gremlinEdgeType)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(sourceProperty);
            variablePropertyList.Add(adjEdge);
            variablePropertyList.Add(adjReverseEdge);
            EdgeType = edgeType;
            GremlinEdgeType = gremlinEdgeType;
        }
    }
}
