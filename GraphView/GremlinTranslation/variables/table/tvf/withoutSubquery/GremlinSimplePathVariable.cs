using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSimplePathVariable : GremlinTableVariable
    {
        public GremlinPathVariable PathVariable { get; set; }

        public GremlinSimplePathVariable(GremlinPathVariable pathVariable): base(GremlinVariableType.Table)
        {
            PathVariable = pathVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(PathVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SimplePath, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
