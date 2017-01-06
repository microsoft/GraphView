using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDerivedTableVariable: GremlinTableVariable
    {
        public GremlinToSqlContext SubqueryContext;

        public GremlinDerivedTableVariable(GremlinToSqlContext subqueryContext)
        {
            SubqueryContext = subqueryContext;
            VariableName = GenerateTableAlias();
        }

        public override WTableReference ToTableReference()
        {
            return SqlUtil.GetDerivedTable(SubqueryContext.ToSelectQueryBlock(ProjectedProperties), VariableName);
        }
    }
}
