using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal enum HasOpType
    {
        HasKey,
        HasKeyValue,
        HasLabelKeyValue,
        HasLabelKeyPredicate,
        HasKeyPredicate,
        HasKeyTraversal,

        HasId,
        HasKeys,
        HasLabel,
        HasValue
    }
    internal class GremlinHasOp: GremlinTranslationOperator
    {
        public string PropertyKey { get; set; }
        public object Value { get; set; }
        public List<object> Values { get; set; }
        public List<string> Keys { get; set; }
        public string Label { get; set; }
        public Predicate Predicate { get; set; }
        public GraphTraversal2 Traversal { get; set; }
        public HasOpType OpType { get; set; }

        public GremlinHasOp(string propertyKey)
        {
            PropertyKey = propertyKey;
            OpType = HasOpType.HasKey;
        }

        public GremlinHasOp(string propertyKey, object value)
        {
            PropertyKey = propertyKey;
            Value = value;
            OpType = HasOpType.HasKeyValue;
        }

        public GremlinHasOp(string label, string propertyKey, object value)
        {
            Label = label;
            PropertyKey = propertyKey;
            Value = value;
            OpType = HasOpType.HasLabelKeyValue;
        }

        public GremlinHasOp(string propertyKey, Predicate predicate)
        {
            PropertyKey = propertyKey;
            Predicate = predicate;
            OpType = HasOpType.HasKeyPredicate;
        }


        public GremlinHasOp(string propertyKey, GraphTraversal2 traversal)
        {
            PropertyKey = propertyKey;
            Traversal = traversal;
            OpType = HasOpType.HasKeyTraversal;
        }

        public GremlinHasOp(HasOpType type, params object[] values)
        {
            Values = new List<object>(values);
            OpType = type;
        }

        public GremlinHasOp(HasOpType type, params string[] keys)
        {
            Keys = new List<string>(keys);
            OpType = type;
        }

        public GremlinHasOp(string label, string propertyKey, Predicate predicate)
        {
            Label = label;
            PropertyKey = propertyKey;
            Predicate = predicate;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            switch (OpType)
            {
                //has(key)
                case HasOpType.HasKey:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey);
                    break;

                //has(key, value)
                case HasOpType.HasKeyValue:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey, Value);
                    break;

                //has(key, predicate)
                case HasOpType.HasKeyPredicate:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey, Predicate);
                    break;

                //has(label, key, value)
                case HasOpType.HasLabelKeyValue:
                    inputContext.PivotVariable.Has(inputContext, Label, PropertyKey, Value);
                    break;

                //has(label, key, predicate)
                case HasOpType.HasLabelKeyPredicate:
                    inputContext.PivotVariable.Has(inputContext, Label, PropertyKey, Predicate);
                    break;

                case HasOpType.HasKeyTraversal:
                    throw new NotImplementedException();

                //hasId(values)
                case HasOpType.HasId:
                    inputContext.PivotVariable.HasId(inputContext, Values);
                    break;

                //hasKey(values)
                case HasOpType.HasKeys:
                    inputContext.PivotVariable.HasKey(inputContext, Keys);
                    break;

                //hasLabel(values)
                case HasOpType.HasLabel:
                    inputContext.PivotVariable.HasLabel(inputContext, Values);
                    break;

                //hasValue(values)
                case HasOpType.HasValue:
                    inputContext.PivotVariable.HasValue(inputContext, Values);
                    break;

            }

            return inputContext;
        }

    }
}
