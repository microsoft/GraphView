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
        private GremlinVariableProperty vertexId;

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(vertexId.ToScalarExpression());
            Populate("id");
            foreach (var property in ProjectedProperties)
            {
                PropertyKeys.Add(SqlUtil.GetValueExpr(property));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.OutV, PropertyKeys, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty vertexId)
        {
            this.vertexId = vertexId;
        }
    }
}
