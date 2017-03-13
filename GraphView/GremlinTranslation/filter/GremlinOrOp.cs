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
        public GraphTraversal2 FirstTraversal { get; set; }
        public GraphTraversal2 SecondTraversal { get; set; }
        public bool IsInfix { get; set; }

        public GremlinOrOp(params GraphTraversal2[] orTraversals)
        {
            OrTraversals = new List<GraphTraversal2>(orTraversals);
        }

        public GremlinOrOp(GraphTraversal2 firsTraversal, GraphTraversal2 secondTraversal)
        {
            FirstTraversal = firsTraversal;
            SecondTraversal = secondTraversal;
            IsInfix = true;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = InputOperator.GetContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            List<GremlinToSqlContext> orContexts = new List<GremlinToSqlContext>();
            if (IsInfix)
            {
                FirstTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                orContexts.Add(FirstTraversal.GetEndOp().GetContext());

                SecondTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                orContexts.Add(SecondTraversal.GetEndOp().GetContext());
            }
            else
            {
                foreach (var orTraversal in OrTraversals)
                {
                    orTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                    orContexts.Add(orTraversal.GetEndOp().GetContext());
                }
            }

            inputContext.PivotVariable.Or(inputContext, orContexts);

            return inputContext;
        }
    }
}
