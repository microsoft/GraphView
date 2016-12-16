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
        public GraphTraversal2 WhereTraversal;

        public GremlinWhereOp(Predicate predicate)
        {
            Predicate = predicate;
        }
        public GremlinWhereOp(string startKey, Predicate predicate)
        {
            StartKey = startKey;
            Predicate = predicate;
        }

        public GremlinWhereOp(GraphTraversal2 whereTraversal)
        {
            WhereTraversal = whereTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVariable currVar = inputContext.CurrVariable;

            if (Predicate != null && Predicate.IsAliasValue)
            {
                Predicate.VariableName = inputContext.AliasToGremlinVariableList[Predicate.Value as string].Last().VariableName;
            }

            if (WhereTraversal == null && StartKey == null)
            {
                //where(Predicate)
                //use Predicates
                WScalarExpression key = GremlinUtil.GetColumnReferenceExpression(currVar.VariableName, "id");
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, Predicate);
                inputContext.AddPredicate(booleanExpr);
            }
            else if (WhereTraversal == null && StartKey != null)
            {
                //where(StartKey, Predicate)
                WScalarExpression key = GremlinUtil.GetColumnReferenceExpression(currVar.VariableName, StartKey);
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, Predicate);
                inputContext.AddPredicate(booleanExpr);
            }
            else
            {
                //where(whereTraversal)
                //use Exist
                GremlinUtil.InheritedVariableFromParent(WhereTraversal, inputContext);

                GremlinToSqlContext subQueryContext = WhereTraversal.GetEndOp().GetContext();
                WBooleanExpression existPredicate = subQueryContext.ToSqlBoolean();

                inputContext.AddPredicate(existPredicate);
            }

            return inputContext;
        }
    }
}
