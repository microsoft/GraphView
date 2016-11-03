using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GramlinTranslationOperator
{
    internal class GremlinAndOp : GremlinTranslationOperator
    {
        public IList<GremlinTranslationOperator> ConjunctiveOperators { get; set; }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = InputOperator.GetContext();
            WBooleanExpression andExpression = null;

            foreach (GremlinTranslationOperator predicateOp in ConjunctiveOperators)
            {
                // Traces to the root of the inner translation chain
                var rootOp = predicateOp;
                while (rootOp.InputOperator != null)
                {
                    rootOp = rootOp.InputOperator;
                }

                // Inputs the outer context into the inner translaiton chain, 
                // if the inner translation chain references the outer context
                if (rootOp.GetType() == typeof(GremlinParentContextOp))
                {
                    GremlinParentContextOp rootAsContext = rootOp as GremlinParentContextOp;
                    rootAsContext.InheritedVariable = inputContext.LastVariable;
                }

                GremlinToSqlContext booleanContext = predicateOp.GetContext();
                WBooleanExpression booleanSql = booleanContext.ToSqlBoolean();

                // Constructs a conjunctive boolean expression
                andExpression = andExpression == null ? booleanSql :
                    new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.And,
                        FirstExpr = andExpression,
                        SecondExpr = booleanSql
                    };
            }

            // Puts andExpression into inputContext
            GremlinVariable target = inputContext.LastVariable;
            if (inputContext.VariablePredicates.ContainsKey(target))
            {
                inputContext.VariablePredicates[target] = new WBooleanBinaryExpression()
                {
                    BooleanExpressionType = BooleanBinaryExpressionType.And,
                    FirstExpr = inputContext.VariablePredicates[target],
                    SecondExpr = andExpression
                };
            }
            else
            {
                inputContext.VariablePredicates[target] = andExpression;
            }

            return inputContext;
        }
    }
}
