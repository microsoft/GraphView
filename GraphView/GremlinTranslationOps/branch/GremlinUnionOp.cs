using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionOp: GremlinTranslationOperator
    {
        public List<GraphTraversal2> UnionTraversals { get; set; }

        public GremlinUnionOp(params GraphTraversal2[] unionTraversals)
        {
            UnionTraversals = new List<GraphTraversal2>();
            foreach (var unionTraversal in unionTraversals)
            {
                UnionTraversals.Add(unionTraversal);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            WQueryDerivedTable queryDerivedTable = null;
            WBinaryQueryExpression binaryQueryExpression = null;
            if (UnionTraversals.Count == 0)
            {
                throw new NotImplementedException();
            }
            if (UnionTraversals.Count == 1)
            {
                GremlinUtil.InheritedContextFromParent(UnionTraversals.First(), inputContext);
                GremlinToSqlContext context = UnionTraversals.First().GetEndOp().GetContext();
                if (!(UnionTraversals.First().GetStartOp() is GremlinParentContextOp))
                {
                    foreach (var statement in context.Statements)
                    {
                        inputContext.Statements.Add(statement);
                    }
                }
                return inputContext;
            }

            List<WSelectQueryBlock> sqlStatements = new List<WSelectQueryBlock>();
      
            foreach (var traversal in UnionTraversals)
            {
                inputContext.SaveCurrentState();

                GremlinUtil.InheritedContextFromParent(traversal, inputContext);
                GremlinToSqlContext context = traversal.GetEndOp().GetContext();
                if (!(traversal.GetStartOp() is GremlinParentContextOp))
                {
                    foreach (var s in context.Statements)
                    {
                        inputContext.Statements.Add(s);
                    }
                }
                WSqlStatement statement = context.ToSqlStatement();
                if (statement is WSelectQueryBlock)
                {
                    sqlStatements.Add(statement as WSelectQueryBlock);
                }
                else
                {
                    var setVarStatement = GremlinUtil.GetSetVariableStatement(context.CurrVariable, statement);
                    inputContext.Statements.Add(setVarStatement);
                    sqlStatements.Add(GremlinUtil.GetSelectQueryBlockFromVariableStatement(setVarStatement));
                }
                inputContext.ResetSavedState();
            }

            binaryQueryExpression = new WBinaryQueryExpression()
            {
                FirstQueryExpr = sqlStatements[0],
                SecondQueryExpr = sqlStatements[1],
                All = true,
                BinaryQueryExprType = BinaryQueryExpressionType.Union,
            };
            for (var i = 2; i < sqlStatements.Count; i++)
            {
                binaryQueryExpression = new WBinaryQueryExpression()
                {
                    FirstQueryExpr = binaryQueryExpression,
                    SecondQueryExpr = sqlStatements[i],
                    All = true,
                    BinaryQueryExprType = BinaryQueryExpressionType.Union,
                };
            }
            //Todo: If we should set the union as a VariableReference?
            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(binaryQueryExpression, "union");
            inputContext.AddNewVariable(newVariable);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
