using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinConstantOp: GremlinTranslationOperator
    {
        public object Constant { get; set; }

        public GremlinConstantOp(object constant)
        {
            Constant = constant;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.Constant(inputContext, Constant);

            return inputContext;
        }
    }
}
