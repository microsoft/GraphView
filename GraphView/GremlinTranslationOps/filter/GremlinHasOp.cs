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

            foreach (var currVar in inputContext.CurrVariableList)
            {

                if (OpType == HasOpType.hasKey)
                {
                    //has(key)
                    WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(currVar, "type", Key);
                    inputContext.AddPredicate(booleanExpr);
                }
                else if (OpType == HasOpType.hasKeyValue)
                {
                    //has(key, value)
                    WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(currVar, Key, Value);
                    inputContext.AddPredicate(booleanExpr);
                }
                else if (OpType == HasOpType.hasKeyPredicate)
                {
                    //has(key, predicate)
                    WBooleanExpression booleanExpr = GremlinUtil.GetBooleanComparisonExpr(currVar, Key, Predicate);
                    inputContext.AddPredicate(booleanExpr);
                }
                else if (OpType == HasOpType.hasLabelKeyValue)
                {
                    //has(label, key, value)
                    WBooleanExpression booleanExpr1 = GremlinUtil.GetBooleanComparisonExpr(currVar, "type", Label);
                    WBooleanExpression booleanExpr2 = GremlinUtil.GetBooleanComparisonExpr(currVar, Key, Value);
                    WBooleanExpression booleanExprBoth = GremlinUtil.GetAndBooleanBinaryExpr(booleanExpr1, booleanExpr2);
                    inputContext.AddPredicate(booleanExprBoth);
                }
                else if (OpType == HasOpType.hasKeyTraversal)
                {
                    //has(key, traversal)

                }
            }
            return inputContext;
        }

    }
}
