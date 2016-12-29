using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
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

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            var chooseExpr = new WChoose2() {ChooseDict = new Dictionary<WScalarExpression, WSqlStatement>()};
            WScalarExpression trueExpr = GremlinUtil.GetColumnReferenceExpression("true");
            WScalarExpression falseExpr = GremlinUtil.GetColumnReferenceExpression("false");

            switch (Type)
            {
                case ChooseType.Predicate:
                    var value = (inputContext.ProjectionList.First() as ColumnProjection).Key;
                    WScalarExpression key = GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.VariableName, value);
                    var predicateExpr = GremlinUtil.GetBooleanComparisonExpr(key, Predicate);
                    chooseExpr.PredicateExpr = predicateExpr;
                    chooseExpr.ChooseDict[trueExpr] = TrueChoiceTraversal.GetEndOp().GetContext().ToSelectQueryBlock();
                    chooseExpr.ChooseDict[falseExpr] = FalseChocieTraversal.GetEndOp().GetContext().ToSelectQueryBlock();
                    break;
                case ChooseType.TraversalPredicate:
                    //Move the context to the choice traversal
                    GremlinUtil.InheritedVariableFromParent(PredicateTraversal, inputContext);
                    chooseExpr.ChooseSqlStatement = PredicateTraversal.GetEndOp().GetContext().ToSelectQueryBlock();

                    //create different branch context
                    GremlinUtil.InheritedVariableFromParent(TrueChoiceTraversal, inputContext);
                    GremlinUtil.InheritedVariableFromParent(FalseChocieTraversal, inputContext);
                    chooseExpr.ChooseDict[trueExpr] = TrueChoiceTraversal.GetEndOp().GetContext().ToSelectQueryBlock();
                    chooseExpr.ChooseDict[falseExpr] = FalseChocieTraversal.GetEndOp().GetContext().ToSelectQueryBlock();
                    break;

                case ChooseType.Option:
                    //Move the context to the choice traversal
                    GremlinUtil.InheritedVariableFromParent(ChoiceTraversal, inputContext);
                    chooseExpr.ChooseSqlStatement = ChoiceTraversal.GetEndOp().GetContext().ToSelectQueryBlock();

                    //create different branch context
                    foreach (var option in OptionDict)
                    {
                        var valueExpr = GremlinUtil.GetValueExpression(option.Key);
                        var optionTraversal = option.Value;

                        GremlinUtil.InheritedVariableFromParent(optionTraversal, inputContext);
                        chooseExpr.ChooseDict[valueExpr] = optionTraversal.GetEndOp().GetContext().ToSelectQueryBlock();
                    }
                    break;
            }
            //Pack the WChoose to a GremlinVariable
            //GremlinChooseVariable newVariable = new GremlinChooseVariable(chooseExpr);
            //inputContext.AddNewVariable(newVariable);
            throw new NotImplementedException();

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
