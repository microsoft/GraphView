using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.branch
{
    internal class GremlinUnionOp: GremlinTranslationOperator
    {
        public List<GraphTraversal2> UnionTraversals;

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
                
            }
            else if (UnionTraversals.Count == 1)
            {
                queryDerivedTable = new WQueryDerivedTable()
                {
                    QueryExpr = UnionTraversals.First().GetEndOp().GetContext().ToSqlQuery() as WSelectQueryBlock 
                };
            }
            else
            {
                binaryQueryExpression = new WBinaryQueryExpression()
                {
                    FirstQueryExpr = UnionTraversals[0].GetEndOp().GetContext().ToSqlQuery() as WSelectQueryBlock,
                    SecondQueryExpr = UnionTraversals[1].GetEndOp().GetContext().ToSqlQuery() as WSelectQueryBlock,
                    All = true,
                    BinaryQueryExprType = BinaryQueryExpressionType.Union,
                };
                for (var i = 2; i < UnionTraversals.Count; i++)
                {
                    binaryQueryExpression = new WBinaryQueryExpression()
                    {
                        FirstQueryExpr = binaryQueryExpression,
                        SecondQueryExpr = UnionTraversals[i].GetEndOp().GetContext().ToSqlQuery() as WSelectQueryBlock,
                        All = true,
                        BinaryQueryExprType = BinaryQueryExpressionType.Union,
                    };
                }
                queryDerivedTable = new WQueryDerivedTable()
                {
                    QueryExpr = binaryQueryExpression
                };
            }
             

            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(queryDerivedTable);
            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
