using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCommitVariable : GremlinTableVariable
    {
        public GremlinCommitVariable() : base(GremlinVariableType.Table) { }

        public override WTableReference ToTableReference()
        {
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Commit, new List<WScalarExpression>(), GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
