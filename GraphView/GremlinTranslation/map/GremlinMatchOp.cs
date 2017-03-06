using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinMatchOp : GremlinTranslationOperator
    {
        public List<GraphTraversal2> MatchTraversals { get; set; }
        public Dictionary<string, List<GraphTraversal2>> MatchTraversalsDict { get; set; }

        public GremlinMatchOp(params GraphTraversal2[] matchTraversals)
        {
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }
            throw new NotImplementedException();
        }
    }
}
