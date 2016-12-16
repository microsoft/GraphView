using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinOptionalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 TraversalOption;

        public GremlinOptionalOp(GraphTraversal2 traversalOption)
        {
            TraversalOption = traversalOption;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinToSqlContext existOptionQueryContext = inputContext.Copy();
            GremlinToSqlContext notexistOptionQueryContext = inputContext.Copy();
            GraphTraversal2 traversal;

            traversal = TraversalOption.Copy();
            GremlinUtil.InheritedContextFromParent(traversal, existOptionQueryContext);
            var existOptionQueryExpr = traversal.GetEndOp().GetContext().ToSelectQueryBlock();

            traversal = TraversalOption.Copy();
            GremlinUtil.InheritedVariableFromParent(traversal, notexistOptionQueryContext);
            var optionQueryExpr = traversal.GetEndOp().GetContext().ToSelectQueryBlock();
            var notExistBooleanExpr = GremlinUtil.GetNotExistPredicate(optionQueryExpr);
            notexistOptionQueryContext.AddPredicate(notExistBooleanExpr);
            var notExistOptionQueryExpr = notexistOptionQueryContext.ToSelectQueryBlock();

            var unionExpr = new WBinaryQueryExpression()
            {
                FirstQueryExpr = existOptionQueryExpr,
                SecondQueryExpr = notExistOptionQueryExpr,
                All = true,
                BinaryQueryExprType = BinaryQueryExpressionType.Union,
            };

            inputContext.ClearAndCreateNewContextInfo();
            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(unionExpr, "optional");
            inputContext.AddNewVariable(newVariable);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
