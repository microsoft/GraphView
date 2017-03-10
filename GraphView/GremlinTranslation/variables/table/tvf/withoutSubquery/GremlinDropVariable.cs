using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDropVariable : GremlinTableVariable
    {
        public GremlinVariable DroppedVariable { get; set; }

        public GremlinDropVariable(GremlinVariable droppedVariable) : base(GremlinVariableType.NULL)
        {
            DroppedVariable = droppedVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(DroppedVariable.DefaultProjection().ToScalarExpression());
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Drop, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
