using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinNotOp: GremlinTranslationOperator
    {
        public GraphTraversal NotTraversal { get; set; }

        public GremlinNotOp(GraphTraversal notTraversal)
        {
            NotTraversal = notTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of not()-step can't be null.");
            }

            NotTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext notContext = NotTraversal.GetEndOp().GetContext();

            inputContext.PivotVariable.Not(inputContext, notContext);

            return inputContext;
        }
    }
}
