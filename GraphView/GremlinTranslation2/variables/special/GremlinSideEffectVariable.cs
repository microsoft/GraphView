using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSideEffectVariable: GremlinVariable, ISqlStatement
    {
        public GremlinToSqlContext Context;

        public GremlinSideEffectVariable(GremlinToSqlContext context)
        {
            Context = context;
        }

        public List<WSqlStatement> ToSetVariableStatements()
        {
            return Context.GetSetVariableStatements();
        }
    }
}
