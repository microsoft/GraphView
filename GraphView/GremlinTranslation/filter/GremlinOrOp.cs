using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOrOp : GremlinTranslationOperator
    {
        public List<GraphTraversal2> OrTraversals { get; set; }

        public GremlinOrOp(params GraphTraversal2[] orTraversals)
        {
            OrTraversals = new List<GraphTraversal2>(orTraversals);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = InputOperator.GetContext();

            List<GremlinToSqlContext> andContexts = new List<GremlinToSqlContext>();
            foreach (var orTraversal in OrTraversals)
            {
                orTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                andContexts.Add(orTraversal.GetEndOp().GetContext());
            }

            inputContext.PivotVariable.Or(inputContext, andContexts);

            return inputContext;
        }
    }
}
