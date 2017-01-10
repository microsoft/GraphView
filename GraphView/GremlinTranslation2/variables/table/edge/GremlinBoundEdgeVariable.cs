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
            bool isBothForward = false;
            if (variablePropertyList.Count == 3)
            {
                isBothForward = true;
            }
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
            if (isBothForward)
            {
                secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothForwardE, PropertyKeys, this, VariableName);
            }
            else {
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
            }

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty adjEdge, WEdgeType edgeType)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(adjEdge);
            EdgeType = edgeType;
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty adjEdge, GremlinVariableProperty adjReverseEdge, WEdgeType edgeType)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(adjEdge);
            variablePropertyList.Add(adjReverseEdge);
            EdgeType = edgeType;
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty sourceNode, GremlinVariableProperty adjEdge, GremlinVariableProperty adjReverseEdge, WEdgeType edgeType)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(sourceNode);
            variablePropertyList.Add(adjEdge);
            variablePropertyList.Add(adjReverseEdge);
            EdgeType = edgeType;
        }
    }
}
