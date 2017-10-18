using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinCyclicPathOp : GremlinTranslationOperator
    {
        public string FromLabel { get; set; }
        public string ToLabel { get; set; }
        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of cyclicPath()-step can't be null.");
            }

            inputContext.PivotVariable.CyclicPath(inputContext, FromLabel, ToLabel);

            return inputContext;
        }
    }
}
