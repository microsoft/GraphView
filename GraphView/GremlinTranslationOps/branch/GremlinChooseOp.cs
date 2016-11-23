using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.branch
{
    internal class GremlinChooseOp: GremlinTranslationOperator
    {
        public GremlinTranslationOperator PredicateOperator;
        public GremlinTranslationOperator TrueChoiceOperator;
        public GremlinTranslationOperator FalseChocieOperator;
        public GremlinTranslationOperator ChoiceOperator;
        public Predicate Predicate;
        public ChooseType Type;
        public Dictionary<object, GraphTraversal2> OptionDict;

        public GremlinChooseOp(GraphTraversal2 traversalPredicate, GraphTraversal2 trueChoice,
            GraphTraversal2 falseChoice)
        {
            PredicateOperator = traversalPredicate.LastGremlinTranslationOp;
            TrueChoiceOperator = trueChoice.LastGremlinTranslationOp;
            FalseChocieOperator = falseChoice.LastGremlinTranslationOp;
            Type = ChooseType.TraversalPredicate;
            OptionDict = new Dictionary<object, GraphTraversal2>();
        }

        public GremlinChooseOp(GraphTraversal2 choiceTraversal)
        {
            ChoiceOperator = choiceTraversal.LastGremlinTranslationOp;
            Type = ChooseType.Option;
            OptionDict = new Dictionary<object, GraphTraversal2>();
        }

        public GremlinChooseOp(Predicate predicate, GraphTraversal2 trueChoice, GraphTraversal2 falseChoice)
        {
            Predicate = predicate;
            TrueChoiceOperator = trueChoice.LastGremlinTranslationOp;
            FalseChocieOperator = falseChoice.LastGremlinTranslationOp;
            Type = ChooseType.Predicate;
            OptionDict = new Dictionary<object, GraphTraversal2>();
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            var chooseExpr = new WChoose2() {ChooseDict = new Dictionary<WScalarExpression, WSqlStatement>()};
            WScalarExpression trueExpr = GremlinUtil.GetColumnReferenceExpression("true");
            WScalarExpression falseExpr = GremlinUtil.GetColumnReferenceExpression("false");

            switch (Type)
            {
                case ChooseType.Predicate:
                    var value = (inputContext.Projection.First().Item2 as ValueProjection).Value;
                    var predicateExpr = GremlinUtil.GetBooleanComparisonExpr(inputContext.CurrVariable, value,
                        Predicate);
                    chooseExpr.PredicateExpr = predicateExpr;
                    chooseExpr.ChooseDict[trueExpr] = TrueChoiceOperator.GetContext().ToSqlQuery();
                    chooseExpr.ChooseDict[falseExpr] = FalseChocieOperator.GetContext().ToSqlQuery();
                    break;
                case ChooseType.TraversalPredicate:
                    //Move the context to the choice traversal
                    GremlinUtil.InheritedVariableFromParent(PredicateOperator, inputContext);
                    chooseExpr.ChooseSqlStatement = PredicateOperator.GetContext().ToSqlQuery();

                    //create different branch context
                    GremlinUtil.InheritedVariableFromParent(TrueChoiceOperator, inputContext);
                    GremlinUtil.InheritedVariableFromParent(FalseChocieOperator, inputContext);
                    chooseExpr.ChooseDict[trueExpr] = TrueChoiceOperator.GetContext().ToSqlQuery();
                    chooseExpr.ChooseDict[falseExpr] = FalseChocieOperator.GetContext().ToSqlQuery();
                    break;

                case ChooseType.Option:
                    //Move the context to the choice traversal
                    GremlinUtil.InheritedVariableFromParent(ChoiceOperator, inputContext);
                    chooseExpr.ChooseSqlStatement = ChoiceOperator.GetContext().ToSqlQuery();

                    //create different branch context
                    foreach (var option in OptionDict)
                    {
                        var valueExpr = GremlinUtil.GetValueExpression(option.Key);
                        var op = option.Value.LastGremlinTranslationOp;

                        GremlinUtil.InheritedVariableFromParent(op, inputContext);
                        chooseExpr.ChooseDict[valueExpr] = op.GetContext().ToSqlQuery();
                    }
                    break;
            }
            //Pack the WChoose to a GremlinVariable
            GremlinChooseVariable newVariable = new GremlinChooseVariable(chooseExpr);
            inputContext.AddNewVariable(newVariable, Labels);

            return inputContext;
        }

        public enum ChooseType
        {
            TraversalPredicate,
            Predicate,
            Option
        }

    }
}
