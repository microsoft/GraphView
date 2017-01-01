using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinInjectOp: GremlinTranslationOperator
    {
        public object[] Injections { get; set; }

        public GremlinInjectOp(params object[] injections)
        {
            Injections = injections;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }
    }
}
