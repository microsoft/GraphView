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
        private List<GremlinVariableProperty> variablePropertyList;

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            bool isBothV = variablePropertyList.Count == 2;
            foreach (var variableProperty in variablePropertyList)
            {
                PropertyKeys.Add(variableProperty.ToScalarExpression());
            }
            foreach (var property in ProjectedProperties)
            {
                PropertyKeys.Add(SqlUtil.GetValueExpr(property));
            }

            WTableReference secondTableRef = null;
            if (isBothV)
            {
                secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothV, PropertyKeys, this, VariableName);
            }
            else if (variablePropertyList.First().GremlinVariable is GremlinEdgeTableVariable)
            {
                switch ((variablePropertyList.First().GremlinVariable as GremlinEdgeTableVariable).EdgeType)
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
            }
            else
            {
                secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutV, PropertyKeys, this, VariableName);
            }
           

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty vertexId)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(vertexId);
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty sourceProperty, GremlinVariableProperty sinkProperty)
        {
            variablePropertyList = new List<GremlinVariableProperty>();
            variablePropertyList.Add(sourceProperty);
            variablePropertyList.Add(sinkProperty);
        }
    }
}
