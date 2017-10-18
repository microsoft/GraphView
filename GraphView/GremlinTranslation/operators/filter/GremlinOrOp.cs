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
        public List<GraphTraversal> OrTraversals { get; set; }
        public GraphTraversal FirstTraversal { get; set; }
        public GraphTraversal SecondTraversal { get; set; }
        public bool IsInfix { get; set; }

        public GremlinOrOp(params GraphTraversal[] orTraversals)
        {
            OrTraversals = new List<GraphTraversal>(orTraversals);
        }

        public GremlinOrOp(GraphTraversal firsTraversal, GraphTraversal secondTraversal)
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
                throw new TranslationException("The PivotVariable of or()-step can't be null.");
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
