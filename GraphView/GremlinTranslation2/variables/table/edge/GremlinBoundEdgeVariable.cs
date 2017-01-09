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
            var secondTableRef = EdgeType == WEdgeType.BothEdge ? SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothE, PropertyKeys, VariableName)
                                                                : SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutE, PropertyKeys, VariableName);
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
    }
}
