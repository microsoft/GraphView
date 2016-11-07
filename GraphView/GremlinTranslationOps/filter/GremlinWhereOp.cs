using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinWhereOp: GremlinTranslationOperator
    {
        public Predicate Predicate;
        public string StartKey;
        public GremlinTranslationOperator ParamOp;

        public GremlinWhereOp(Predicate predicate)
        {
            Predicate = predicate;
        }
        public GremlinWhereOp(string startKey, Predicate predicate)
        {
            StartKey = startKey;
            Predicate = predicate;
        }

        public GremlinWhereOp(GremlinTranslationOperator paramOp)
        {
            ParamOp = paramOp;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (ParamOp == null && StartKey == null)
            {
                //where(Predicate)
                //use Predicates
                foreach (var currVar in inputContext.CurrVariableList)
                {
                    WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(currVar, "id", Predicate);
                    inputContext.AddPredicate(booleanExpr);
                }
            }
            else if (ParamOp == null && StartKey != null)
            {
                //where(StartKey, Predicate)
                foreach (var currVar in inputContext.CurrVariableList)
                {
                    WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(currVar, StartKey, Predicate);
                    inputContext.AddPredicate(booleanExpr);
                }
            }
            else
            {
                //where(whereTraversal)
                //use Exist
                var rootOp = ParamOp;
                while (rootOp.InputOperator != null)
                {
                    rootOp = rootOp.InputOperator;
                }

                if (rootOp.GetType() == typeof(GremlinParentContextOp)) {
                    GremlinParentContextOp rootAsContext = rootOp as GremlinParentContextOp;
                    rootAsContext.InheritedVariable = inputContext.CurrVariableList;
                }

                GremlinToSqlContext subQueryContext = ParamOp.GetContext();
                WBooleanExpression existPredicate = subQueryContext.ToSqlBoolean();

                inputContext.AddPredicate(existPredicate);
            }

            return inputContext;
        }
    }
}
