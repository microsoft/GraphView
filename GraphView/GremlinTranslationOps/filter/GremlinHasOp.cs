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
        public string Value;
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

        public GremlinHasOp(string key, string value)
        {
            Key = key;
            Value = value;
            OpType = HasOpType.hasKeyValue;
        }

        public GremlinHasOp(string label, string key, string value)
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

            if (OpType == HasOpType.hasKey)
            {
                //has(key)

            }
            else if (OpType == HasOpType.hasKeyValue)
            {
                //has(key, value)
            }
            else if (OpType == HasOpType.hasKeyPredicate)
            {
                //has(key, predicate)
            }
            else if (OpType == HasOpType.hasLabelKeyValue)
            {
                //has(label, key, value)
            }
            else if (OpType == HasOpType.hasKeyTraversal)
            {
                //has(key, traversal)
            }
            return inputContext;
        }

    }
}
