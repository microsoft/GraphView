using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation2.variables.special
{
    internal class GremlinSideEffectVariable: GremlinVariable2, ISqlStatement
    {
        public GremlinToSqlContext Context;

        public GremlinSideEffectVariable(GremlinToSqlContext context)
        {
            Context = context;
        }

        public List<WSqlStatement> ToSetVariableStatements()
        {
            return Context.GetStatements();
        }
    }
}
