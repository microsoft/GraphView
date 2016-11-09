using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinHasOp: GremlinTranslationOperator
    {
        public string Key;
        public object Value;
        public object Values;
        public string Label;
        public Predicate Predicate;
        public GremlinTranslationOperator ParamOp;
        public HasOpType OpType;

        internal enum HasOpType
        {
            HasKey,
            HasKeyValue,
            HasLabelKeyValue,
            HasKeyPredicate,
            HasKeyTraversal,

            HasId,
            HasKeys,
            HasLabel,
            HasValue
        }
        public GremlinHasOp(string key)
        {
            Key = key;
            OpType = HasOpType.HasKey;
        }
        public GremlinHasOp(string key, object value)
        {
            Key = key;
            Value = value;
            OpType = HasOpType.HasKeyValue;
        }

        public GremlinHasOp(string label, string key, object value)
        {
            Label = label;
            Key = key;
            Value = value;
            OpType = HasOpType.HasLabelKeyValue;
        }

        public GremlinHasOp(string key, Predicate predicate)
        {
            Key = key;
            Predicate = predicate;
            OpType = HasOpType.HasKeyPredicate;
        }


        public GremlinHasOp(string key, GremlinTranslationOperator paramOp)
        {
            Key = key;
            ParamOp = paramOp;
            OpType = HasOpType.HasKeyTraversal;
        }

        public GremlinHasOp(HasOpType type, params object[] values)
        {
            Values = values;
            OpType = type;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVariable currVar = inputContext.CurrVariable;

            if (OpType == HasOpType.HasKey)
            {
                //has(key)
                WBooleanExpression booleanExpr = GremlinUtil.GetHasKeyBooleanExpression(currVar, Key);

                inputContext.AddPredicate(booleanExpr);
            }
            else if (OpType == HasOpType.HasKeyValue)
            {
                //has(key, value)
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(currVar, Key, Value);
                inputContext.AddPredicate(booleanExpr);
            }
            else if (OpType == HasOpType.HasKeyPredicate)
            {
                //has(key, predicate)
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(currVar, Key, Predicate);
                inputContext.AddPredicate(booleanExpr);
            }
            else if (OpType == HasOpType.HasLabelKeyValue)
            {
                //has(label, key, value)
                WBooleanExpression booleanExpr1 = GremlinUtil.GetBooleanComparisonExpr(currVar, "type", Label);
                WBooleanExpression booleanExpr2 = GremlinUtil.GetBooleanComparisonExpr(currVar, Key, Value);
                WBooleanExpression booleanExprBoth = GremlinUtil.GetAndBooleanBinaryExpr(booleanExpr1, booleanExpr2);
                inputContext.AddPredicate(booleanExprBoth);
            }
            else if (OpType == HasOpType.HasKeyTraversal)
            {
                //has(key, traversal)
                var rootOp = ParamOp;
                while (rootOp.InputOperator != null)
                {
                    rootOp = rootOp.InputOperator;
                }

                if (rootOp.GetType() == typeof(GremlinParentContextOp))
                {
                    GremlinParentContextOp rootAsContext = rootOp as GremlinParentContextOp;
                    rootAsContext.InheritedVariable = inputContext.CurrVariable;
                }

                GremlinToSqlContext booleanContext = ParamOp.GetContext();
                WBooleanExpression booleanSql = booleanContext.ToSqlBoolean();

                inputContext.AddPredicate(booleanSql);
            }
            else if (OpType == HasOpType.HasId)
            {
                List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                foreach (var value in Values as string[])
                {
                    booleanExprList.Add(GremlinUtil.GetBooleanComparisonExpr(currVar, "id", value));
                }
                WBooleanExpression concatSql = GremlinUtil.ConcatBooleanExpressionListWithOr(booleanExprList);
                inputContext.AddPredicate(concatSql);
            }
            else if (OpType == HasOpType.HasKeys)
            {
                foreach (var key in Values as string[])
                {
                    WBooleanExpression booleanExpr = GremlinUtil.GetHasKeyBooleanExpression(currVar, key);
                    inputContext.AddPredicate(booleanExpr);
                }
            }
            else if (OpType == HasOpType.HasLabel)
            {
                List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                foreach (var value in Values as string[])
                {
                    booleanExprList.Add(GremlinUtil.GetBooleanComparisonExpr(currVar, "type", value));
                }
                WBooleanExpression concatSql = GremlinUtil.ConcatBooleanExpressionListWithOr(booleanExprList);
                inputContext.AddPredicate(concatSql);
            }
            else if (OpType == HasOpType.HasValue)
            {

            }
            return inputContext;
        }

    }
}
