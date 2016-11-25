using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinIsOp: GremlinTranslationOperator
    {
        public object Value;
        public Predicate Predicate;
        public IsType Type;

        public GremlinIsOp(object value)
        {
            Value = value;
            Type = IsType.IsValue;
        }

        public GremlinIsOp(Predicate predicate)
        {
            Predicate = predicate;
            Type = IsType.IsPredicate;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinVariable currVariable = inputContext.CurrVariable;
            WScalarExpression key = GremlinUtil.GetColumnReferenceExpression(currVariable.VariableName);
            if (Type == IsType.IsValue)
            {
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, Value);
                inputContext.AddPredicate(booleanExpr);
            }
            else if (Type == IsType.IsPredicate)
            {
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, Predicate);
                inputContext.AddPredicate(booleanExpr);
            }

            return inputContext;
        }

        public enum IsType
        {
            IsValue,
            IsPredicate
        }
    }
}
