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
        public GraphTraversal PredicateTraversal { get; set; }
        public GraphTraversal TrueChoiceTraversal { get; set; }
        public GraphTraversal FalseChocieTraversal { get; set; }
        public GraphTraversal ChoiceTraversal { get; set; }
        public Dictionary<object, GraphTraversal> Options { get; set; }

        public GremlinChooseOp(GraphTraversal traversalPredicate, GraphTraversal trueChoice,
            GraphTraversal falseChoice)
        {
            PredicateTraversal = traversalPredicate;
            TrueChoiceTraversal = trueChoice;
            FalseChocieTraversal = falseChoice;
            Options = new Dictionary<object, GraphTraversal>();
        }

        public GremlinChooseOp(GraphTraversal choiceTraversal)
        {
            ChoiceTraversal = choiceTraversal;
            Options = new Dictionary<object, GraphTraversal>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of choose()-step can't be null.");
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
