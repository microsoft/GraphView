using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinOrOp : GremlinTranslationOperator
    {
        public IList<GremlinTranslationOperator> ConjunctiveOperators { get; set; }

        public GremlinOrOp(params GraphTraversal2[] andTraversals)
        {
            ConjunctiveOperators = new List<GremlinTranslationOperator>();
            foreach (var traversal in andTraversals)
            {
                ConjunctiveOperators.Add(traversal.LastGremlinTranslationOp);
            }
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = InputOperator.GetContext();
            WBooleanExpression orExpression = null;

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
                    rootAsContext.InheritedVariable = inputContext.CurrVariable;
                }

                GremlinToSqlContext booleanContext = predicateOp.GetContext();
                WBooleanExpression booleanSql = booleanContext.ToSqlBoolean();

                // Constructs a conjunctive boolean expression
                orExpression = orExpression == null ? booleanSql :
                    new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.Or,
                        FirstExpr = orExpression,
                        SecondExpr = booleanSql
                    };
            }

            // Puts orExpression into inputContext
            inputContext.AddPredicate(GremlinUtil.GetBooleanParenthesisExpression(orExpression)); ;
            return inputContext;
        }
    }
}
