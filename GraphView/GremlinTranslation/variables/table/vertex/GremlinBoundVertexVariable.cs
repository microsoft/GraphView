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

            WTableReference tableRef = null;
            if (SourceVariableProperty != null && SinkVariableProperty != null)
            {
                //BothV
                tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothV, PropertyKeys, GetVariableName());
            }
            switch (InputEdgeType)
            {
                case WEdgeType.BothEdge:
                    tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OtherV, PropertyKeys, GetVariableName());
                    break;
                case WEdgeType.InEdge:
                    tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutV, PropertyKeys, GetVariableName());
                    break;
                case WEdgeType.OutEdge:
                    tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.InV, PropertyKeys, GetVariableName());
                    break;
                case WEdgeType.Undefined:
                    tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutV, PropertyKeys, GetVariableName());
                    break;
            }

            return SqlUtil.GetCrossApplyTableReference(tableRef);
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
    }
}
