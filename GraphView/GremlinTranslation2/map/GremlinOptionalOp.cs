using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOptionalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 TraversalOption { get; set; }

        public GremlinOptionalOp(GraphTraversal2 traversalOption)
        {
            TraversalOption = traversalOption;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            TraversalOption.GetStartOp().InheritedVariableFromParent(inputContext);
            inputContext.PivotVariable.Optional(inputContext, TraversalOption.GetEndOp().GetContext());

            return inputContext;
        }
    }
}
