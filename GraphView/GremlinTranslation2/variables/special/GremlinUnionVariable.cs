using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionVariable: GremlinVariableReference
    {
        public List<GremlinToSqlContext> UnionContextList { get; set; }

        public GremlinUnionVariable(List<GremlinToSqlContext> unionContextList)
        {
            VariableName = GenerateTableAlias();
            UnionContextList = unionContextList;
        }

        public override List<WSqlStatement> ToSetVariableStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();

            if (UnionContextList.Count == 0)
            {
                throw new NotImplementedException();
            }
            else if (UnionContextList.Count == 1)
            {
                throw new NotImplementedException();
            }
            else
            {
                foreach (var context in UnionContextList)
                {
                    statementList.AddRange(context.GetSetVariableStatements());
                }

                WBinaryQueryExpression binaryQueryExpression = new WBinaryQueryExpression()
                {
                    FirstQueryExpr = UnionContextList[0].ToSelectQueryBlock(),
                    SecondQueryExpr = UnionContextList[1].ToSelectQueryBlock(),
                    All = true,
                    BinaryQueryExprType = BinaryQueryExpressionType.Union,
                };
                for (var i = 2; i < UnionContextList.Count; i++)
                {
                    binaryQueryExpression = new WBinaryQueryExpression()
                    {
                        FirstQueryExpr = binaryQueryExpression,
                        SecondQueryExpr = UnionContextList[i].ToSelectQueryBlock(),
                        All = true,
                        BinaryQueryExprType = BinaryQueryExpressionType.Union,
                    };
                }

                //TODO:
                // ignore set union-all as a variable reference for IoT test

                var setStatement = new WSetVariableStatement()
                {
                    Expression = new WScalarSubquery()
                    {
                        SubQueryExpr = binaryQueryExpression
                    },
                    Variable = GremlinUtil.GetVariableReference(VariableName)
                };

                statementList.Add(setStatement);
            }

            return statementList;
        }
    }
}
