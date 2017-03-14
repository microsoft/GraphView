using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinChooseOp: GremlinTranslationOperator
    {
        public GraphTraversal2 PredicateTraversal { get; set; }
        public GraphTraversal2 TrueChoiceTraversal { get; set; }
        public GraphTraversal2 FalseChocieTraversal { get; set; }
        public GraphTraversal2 ChoiceTraversal { get; set; }
        public Dictionary<object, GraphTraversal2> Options { get; set; }

        public GremlinChooseOp(GraphTraversal2 traversalPredicate, GraphTraversal2 trueChoice,
            GraphTraversal2 falseChoice)
        {
            PredicateTraversal = traversalPredicate;
            TrueChoiceTraversal = trueChoice;
            FalseChocieTraversal = falseChoice;
            Options = new Dictionary<object, GraphTraversal2>();
        }

        public GremlinChooseOp(GraphTraversal2 choiceTraversal)
        {
            ChoiceTraversal = choiceTraversal;
            Options = new Dictionary<object, GraphTraversal2>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (TrueChoiceTraversal != null && FalseChocieTraversal != null)
            {
                PredicateTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                TrueChoiceTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                FalseChocieTraversal.GetStartOp().InheritedVariableFromParent(inputContext);

                GremlinToSqlContext predicateContext = PredicateTraversal.GetEndOp().GetContext();
                GremlinToSqlContext trueContext = TrueChoiceTraversal.GetEndOp().GetContext();
                GremlinToSqlContext falseContext = FalseChocieTraversal.GetEndOp().GetContext();

                inputContext.PivotVariable.Choose(inputContext, predicateContext, trueContext, falseContext);
            }
            else
            {
                Dictionary<object, GremlinToSqlContext> options = new Dictionary<object, GremlinToSqlContext>();
                foreach (var option in Options)
                {
                    option.Value.GetStartOp().InheritedVariableFromParent(inputContext);
                    GremlinToSqlContext temp = option.Value.GetEndOp().GetContext();
                    options[option.Key] = temp;
                }

                ChoiceTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                GremlinToSqlContext choiceContext = ChoiceTraversal.GetEndOp().GetContext();
                inputContext.PivotVariable.Choose(inputContext, choiceContext, options);
            }

            return inputContext;
        }
    }
}
