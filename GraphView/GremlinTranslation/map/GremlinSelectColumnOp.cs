using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectColumnOp : GremlinTranslationOperator
    {
        public GremlinKeyword.Column Column { get; set; }

        public GremlinSelectColumnOp(GremlinKeyword.Column column)
        {
            Column = column;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.SelectColumn(inputContext, Column);

            return inputContext;
        }
    }
}
