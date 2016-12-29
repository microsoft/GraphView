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
        public GraphTraversal2 NotTraversal { get; set; }

        public GremlinNotOp(GraphTraversal2 notTraversal)
        {
            NotTraversal = notTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedVariableFromParent(NotTraversal, inputContext);

            WBooleanExpression booleanExpr = GremlinUtil.GetNotExistPredicate(NotTraversal.GetEndOp().GetContext().ToSelectQueryBlock());

            inputContext.AddPredicate(booleanExpr);

            return inputContext;
        }
    }
}
