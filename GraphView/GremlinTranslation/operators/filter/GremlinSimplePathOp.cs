using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinSimplePathOp : GremlinTranslationOperator
    {
        public string FromLabel { get; set; }
        public string ToLabel { get; set; }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of simplePath()-step can't be null.");
            }

            inputContext.PivotVariable.SimplePath(inputContext, FromLabel, ToLabel);

            return inputContext;
        }
    }
}
