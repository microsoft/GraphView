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
        public string Label;
        public Predicate Predicate;
        public GremlinTranslationOperator ParamOp;
        public HasOpType OpType;

        internal enum HasOpType
        {
            hasKey,
            hasKeyValue,
            hasLabelKeyValue,
            hasKeyPredicate,
            hasKeyTraversal
        }

        public GremlinHasOp(string key, object value)
        {
            Key = key;
            Value = value;
            OpType = HasOpType.hasKeyValue;
        }

        public GremlinHasOp(string label, string key, object value)
        {
            Label = label;
            Key = key;
            Value = value;
            OpType = HasOpType.hasLabelKeyValue;
        }

        public GremlinHasOp(string key, Predicate predicate)
        {
            Key = key;
            Predicate = predicate;
            OpType = HasOpType.hasKeyPredicate;
        }

        public GremlinHasOp(string key)
        {
            Key = key;
            OpType = HasOpType.hasKey;
        }
        public GremlinHasOp(string key, GremlinTranslationOperator paramOp)
        {
            Key = key;
            ParamOp = paramOp;
            OpType = HasOpType.hasKeyTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVariable currVar = inputContext.LastVariable;

            if (OpType == HasOpType.hasKey)
            {
                //has(key)
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpression(currVar, "type", Key);
                inputContext.AddVariablePredicate(currVar, booleanExpr);
            }
            else if (OpType == HasOpType.hasKeyValue)
            {
                //has(key, value)
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpression(currVar, Key, Value);
                inputContext.AddVariablePredicate(currVar, booleanExpr);
            }
            else if (OpType == HasOpType.hasKeyPredicate)
            {
                //has(key, predicate)
                WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpression(currVar, Key, Predicate);
                inputContext.AddVariablePredicate(currVar, booleanExpr);
            }
            else if (OpType == HasOpType.hasLabelKeyValue)
            {
                //has(label, key, value)
                WBooleanExpression booleanExpr1 = GremlinUtil.GetBooleanComparisonExpression(currVar, "type", Label);
                WBooleanExpression booleanExpr2 = GremlinUtil.GetBooleanComparisonExpression(currVar, Key, Value);
                WBooleanExpression booleanExprBoth = GremlinUtil.GetBooleanBinaryExpression(booleanExpr1, booleanExpr2);
                inputContext.AddVariablePredicate(currVar, booleanExprBoth);
            }
            else if (OpType == HasOpType.hasKeyTraversal)
            {
                //has(key, traversal)
                
            }
            return inputContext;
        }

    }
}
