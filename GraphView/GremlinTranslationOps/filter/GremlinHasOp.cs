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
            hasKey,
            hasKeyValue,
            hasLabelKeyValue,
            hasKeyPredicate,
            hasKeyTraversal,

            hasId,
            hasKeys,
            hasLabel,
            hasValue
        }
        public GremlinHasOp(string key)
        {
            Key = key;
            OpType = HasOpType.hasKey;
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


        public GremlinHasOp(string key, GremlinTranslationOperator paramOp)
        {
            Key = key;
            ParamOp = paramOp;
            OpType = HasOpType.hasKeyTraversal;
        }

        public GremlinHasOp(HasOpType type, params object[] values)
        {
            Values = values;
            OpType = type;
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
                    var rootOp = ParamOp;
                    while (rootOp.InputOperator != null)
                    {
                        rootOp = rootOp.InputOperator;
                    }

                    if (rootOp.GetType() == typeof(GremlinParentContextOp))
                    {
                        GremlinParentContextOp rootAsContext = rootOp as GremlinParentContextOp;
                        rootAsContext.InheritedVariable = inputContext.CurrVariableList;
                    }

                    GremlinToSqlContext booleanContext = ParamOp.GetContext();
                    WBooleanExpression booleanSql = booleanContext.ToSqlBoolean();

                    inputContext.AddPredicate(booleanSql);
                }
                else if (OpType == HasOpType.hasId)
                {
                    List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                    foreach (var value in Values as string[])
                    {
                        booleanExprList.Add(GremlinUtil.GetBooleanComparisonExpr(currVar, "id", value));
                    }
                    WBooleanExpression concatSql = GremlinUtil.ConcatBooleanExpressionListWithOr(booleanExprList);
                    inputContext.AddPredicate(concatSql);
                }
                else if (OpType == HasOpType.hasKeys)
                {
                    
                }
                else if (OpType == HasOpType.hasLabel)
                {
                    List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                    foreach (var value in Values as string[])
                    {
                        booleanExprList.Add(GremlinUtil.GetBooleanComparisonExpr(currVar, "type", value));
                    }
                    WBooleanExpression concatSql = GremlinUtil.ConcatBooleanExpressionListWithOr(booleanExprList);
                    inputContext.AddPredicate(concatSql);
                }
                else if (OpType == HasOpType.hasValue)
                {

                }
            }
            return inputContext;
        }

    }
}
