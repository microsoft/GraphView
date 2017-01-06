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


                WSelectQueryExpression firstQueryExpr = UnionContextList[0].ToSelectQueryBlock();
                WSelectQueryExpression secondQueryExpr = UnionContextList[1].ToSelectQueryBlock();
                var binaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, secondQueryExpr);
                for (var i = 2; i < UnionContextList.Count; i++)
                {
                    firstQueryExpr = binaryQueryExpression;
                    secondQueryExpr = UnionContextList[i].ToSelectQueryBlock();
                    binaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, secondQueryExpr);
                }

                //TODO:
                // ignore set union-all as a variable reference for IoT test

                var setStatement = new WSetVariableStatement()
                {
                    Expression = new WScalarSubquery()
                    {
                        SubQueryExpr = binaryQueryExpression
                    },
                    Variable = SqlUtil.GetVariableReference(VariableName)
                };

                statementList.Add(setStatement);
            }

            return statementList;
        }
    }
}
