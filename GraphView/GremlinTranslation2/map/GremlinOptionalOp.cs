using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOptionalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 TraversalOption { get; set; }

        public GremlinOptionalOp(GraphTraversal2 traversalOption)
        {
            TraversalOption = traversalOption;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //GremlinToSqlContext existOptionQueryContext = inputContext.Copy();
            //GremlinToSqlContext notexistOptionQueryContext = inputContext.Copy();
            //GraphTraversal2 traversal;

            //traversal = TraversalOption.Copy();
            //GremlinUtil.InheritedContextFromParent(traversal, existOptionQueryContext);
            //var existOptionQueryExpr = traversal.GetEndOp().GetContext().ToSelectQueryBlock();

            //traversal = TraversalOption.Copy();
            //GremlinUtil.InheritedVariableFromParent(traversal, notexistOptionQueryContext);
            //var optionQueryExpr = traversal.GetEndOp().GetContext().ToSelectQueryBlock();
            //var notExistBooleanExpr = GremlinUtil.GetNotExistPredicate(optionQueryExpr);
            //notexistOptionQueryContext.AddPredicate(notExistBooleanExpr);
            //var notExistOptionQueryExpr = notexistOptionQueryContext.ToSelectQueryBlock();

            //var unionExpr = new WBinaryQueryExpression()
            //{
            //    FirstQueryExpr = existOptionQueryExpr,
            //    SecondQueryExpr = notExistOptionQueryExpr,
            //    All = true,
            //    BinaryQueryExprType = BinaryQueryExpressionType.Union,
            //};

            //inputContext.ClearAndCreateNewContextInfo();
            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(unionExpr, "optional");
            //inputContext.AddNewVariable(newVariable);
            //inputContext.SetCurrVariable(newVariable);
            //inputContext.SetDefaultProjection(newVariable);

            inputContext.SaveCurrentState();
            GremlinUtil.InheritedVariableFromParent(TraversalOption, inputContext);
            GremlinToSqlContext context = TraversalOption.GetEndOp().GetContext();
            foreach (var statement in context.Statements)
            {
                inputContext.Statements.Add(statement);
            }
            WScalarSubquery ScalarSubquery = new WScalarSubquery()
            {
                SubQueryExpr = context.ToSelectQueryBlock()
            };
            inputContext.ResetSavedState();

            List<object> PropertyKeys = new List<object>();
            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                PropertyKeys.Add(ScalarSubquery);
                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("optional", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(inputContext.CurrVariable, secondTableRef, Labels);
                newVariable.VariableType = context.CurrVariable.GetVariableType();
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);

            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {
                var oldVariable = inputContext.GetSinkNode(inputContext.CurrVariable);
                PropertyKeys.Add(ScalarSubquery);

                var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("optional", PropertyKeys);

                var newVariable = inputContext.CrossApplyToVariable(oldVariable, secondTableRef, Labels);
                newVariable.VariableType = context.CurrVariable.GetVariableType();
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);
            }
            else
            {
                throw new NotImplementedException();
            }

            return inputContext;
        }
    }
}
