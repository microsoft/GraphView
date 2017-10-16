using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAndOp : GremlinTranslationOperator
    {
        public List<GraphTraversal> AndTraversals { get; set; }
        public GraphTraversal FirstTraversal { get; set; }
        public GraphTraversal SecondTraversal { get; set; }
        public bool IsInfix { get; set; }

        public GremlinAndOp(params GraphTraversal[] andTraversals)
        {
            AndTraversals = new List<GraphTraversal>(andTraversals);
        }

        public GremlinAndOp(GraphTraversal firsTraversal, GraphTraversal secondTraversal)
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
                throw new TranslationException("The PivotVariable of and()-step can't be null.");
            }

            List<GremlinToSqlContext> andContexts = new List<GremlinToSqlContext>();
            if (IsInfix)
            {
                FirstTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                andContexts.Add(FirstTraversal.GetEndOp().GetContext());

                SecondTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                andContexts.Add(SecondTraversal.GetEndOp().GetContext());
            }
            else
            {
                foreach (var andTraversal in AndTraversals)
                {
                    andTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                    andContexts.Add(andTraversal.GetEndOp().GetContext());
                }
            }

            inputContext.PivotVariable.And(inputContext, andContexts);

            return inputContext;
        }
    }
}
