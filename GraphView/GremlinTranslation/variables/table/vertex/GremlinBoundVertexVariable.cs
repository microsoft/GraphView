using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundVertexVariable : GremlinVertexTableVariable
    {
        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            WTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.V, parameters, this.GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
