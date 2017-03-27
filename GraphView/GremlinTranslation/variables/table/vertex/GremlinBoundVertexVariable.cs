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
        public GremlinVariableProperty SourceVariableProperty { get; set; }
        public GremlinVariableProperty SinkVariableProperty { get; set; }

        public GremlinBoundVertexVariable()
        {
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty sourceVariableProperty)
        {
            SourceVariableProperty = sourceVariableProperty;
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty sourceVariableProperty, GremlinVariableProperty sinkVariableProperty)
        {
            SourceVariableProperty = sourceVariableProperty;
            SinkVariableProperty = sinkVariableProperty;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            if (SourceVariableProperty != null)
            {
                parameters.Add(SourceVariableProperty.ToScalarExpression());
            }
            if (SinkVariableProperty != null)
            {
                parameters.Add(SinkVariableProperty.ToScalarExpression());
            }
            foreach (var property in ProjectedProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property));
            }

            WTableReference tableRef = null;
            if (SourceVariableProperty != null && SinkVariableProperty != null)
            {
                //BothV(E_0._source, E_0._sink, "name", "age")
                tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.BothV, parameters, GetVariableName());
            }
            else if (SourceVariableProperty != null || SinkVariableProperty != null)
            {
                //EtoV(E_0._sink, "name", "age")
                //EtoV(E_0._source, "name", "age")
                tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.EtoV, parameters, GetVariableName());
            }
            else
            {
                //V("name", "age")
                tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.V, parameters, GetVariableName());
            }

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
