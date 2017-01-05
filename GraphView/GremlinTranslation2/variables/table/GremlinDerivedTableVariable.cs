using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation2.variables.table
{
    internal class GremlinDerivedTableVariable: GremlinTableVariable
    {
        public GremlinToSqlContext SubqueryContext;

        public GremlinDerivedTableVariable(GremlinToSqlContext subqueryContext)
        {
            SubqueryContext = subqueryContext;
        }

        public override WTableReference ToTableReference()
        {
            return GremlinUtil.GetDerivedTable(SubqueryContext.ToSelectQueryBlock(ProjectedProperties), VariableName);
        }
    }
}
