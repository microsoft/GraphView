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
        public Predicate Predicate { get; set; }
        public ChooseType Type { get; set; }
        public Dictionary<object, GraphTraversal2> OptionDict { get; set; }

        public GremlinChooseOp(GraphTraversal2 traversalPredicate, GraphTraversal2 trueChoice,
            GraphTraversal2 falseChoice)
        {
            PredicateTraversal = traversalPredicate;
            TrueChoiceTraversal = trueChoice;
            FalseChocieTraversal = falseChoice;
            Type = ChooseType.TraversalPredicate;
            OptionDict = new Dictionary<object, GraphTraversal2>();
        }

        public GremlinChooseOp(GraphTraversal2 choiceTraversal)
        {
            ChoiceTraversal = choiceTraversal;
            Type = ChooseType.Option;
            OptionDict = new Dictionary<object, GraphTraversal2>();
        }

        public GremlinChooseOp(Predicate predicate, GraphTraversal2 trueChoice, GraphTraversal2 falseChoice)
        {
            Predicate = predicate;
            TrueChoiceTraversal = trueChoice;
            FalseChocieTraversal = falseChoice;
            Type = ChooseType.Predicate;
            OptionDict = new Dictionary<object, GraphTraversal2>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }
            throw new NotImplementedException();
        }

        public enum ChooseType
        {
            TraversalPredicate,
            Predicate,
            Option
        }

    }
}
