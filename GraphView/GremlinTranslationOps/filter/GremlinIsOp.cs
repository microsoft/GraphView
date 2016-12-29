using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinIsOp: GremlinTranslationOperator
    {
        public object Value { get; set; }
        public Predicate Predicate { get; set; }
        public IsType Type { get; set; }

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
            List<WScalarExpression> keyList = new List<WScalarExpression>();
            foreach (var projection in inputContext.ProjectionList)
            {
                WScalarExpression key = null;
                if (projection is ColumnProjection)
                {
                    key = GremlinUtil.GetColumnReferenceExpression(currVariable.VariableName, (projection as ColumnProjection).Key);
                    keyList.Add(key);
                }
                else if (projection is FunctionCallProjection)
                {
                    //TODO
                }
                else if (projection is StarProjection)
                {
                    key = GremlinUtil.GetColumnReferenceExpression(currVariable.VariableName, "_value");
                    keyList.Add(key);
                }
            }
            if (Type == IsType.IsValue)
            {
                foreach (var key in keyList)
                {
                    WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, Value);
                    inputContext.AddPredicate(booleanExpr);
                }
            }
            else if (Type == IsType.IsPredicate)
            {
                foreach (var key in keyList)
                {
                    WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(key, Predicate);
                    inputContext.AddPredicate(booleanExpr);
                }
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
