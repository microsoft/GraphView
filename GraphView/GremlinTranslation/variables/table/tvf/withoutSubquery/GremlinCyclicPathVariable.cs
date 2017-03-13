using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCyclicPathVariable : GremlinTableVariable
    {
        public GremlinPathVariable PathVariable { get; set; }

        public GremlinCyclicPathVariable(GremlinPathVariable pathVariable) : base(GremlinVariableType.Table)
        {
            PathVariable = pathVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(PathVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.CyclicPath, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
