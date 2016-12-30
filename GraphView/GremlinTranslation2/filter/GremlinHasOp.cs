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
        public string Key { get; set; }
        public object Value { get; set; }
        public List<object> Values { get; set; }
        public string Label { get; set; }
        public Predicate Predicate { get; set; }
        public GraphTraversal2 Traversal { get; set; }
        public HasOpType OpType { get; set; }

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


        public GremlinHasOp(string key, GraphTraversal2 traversal)
        {
            Key = key;
            Traversal = traversal;
            OpType = HasOpType.HasKeyTraversal;
        }

        public GremlinHasOp(HasOpType type, params object[] values)
        {
            Values = new List<object>();
            foreach (var value in values)
            {
                Values.Add(value);
            }
            OpType = type;
        }

        public GremlinHasOp(string label, string propertyKey, Predicate predicate)
        {
            Label = label;
            Key = propertyKey;
            Predicate = predicate;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }

    }
}
