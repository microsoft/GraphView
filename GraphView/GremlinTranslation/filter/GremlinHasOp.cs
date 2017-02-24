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
        HasProperty,
        HasPropertyValue,
        HasPropertyPredicate,
        HasPropertyTraversal,
        HasLabelPropertyValue,
        HasLabelPropertyPredicate,

        HasId,
        HasIdPredicate,
        HasKey,
        HasKeyPredicate,
        HasLabel,
        HasLabelPredicate,
        HasValue,
        HasValuePredicate
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
            OpType = HasOpType.HasProperty;
        }

        public GremlinHasOp(string propertyKey, object value)
        {
            PropertyKey = propertyKey;
            Value = value;
            OpType = HasOpType.HasPropertyValue;
        }

        public GremlinHasOp(string label, string propertyKey, object value)
        {
            Label = label;
            PropertyKey = propertyKey;
            Value = value;
            OpType = HasOpType.HasLabelPropertyValue;
        }

        public GremlinHasOp(string propertyKey, Predicate predicate)
        {
            PropertyKey = propertyKey;
            Predicate = predicate;
            OpType = HasOpType.HasPropertyPredicate;
        }

        public GremlinHasOp(string propertyKey, GraphTraversal2 traversal)
        {
            PropertyKey = propertyKey;
            Traversal = traversal;
            OpType = HasOpType.HasPropertyTraversal;
        }

        public GremlinHasOp(HasOpType type, params object[] values)
        {
            Values = new List<object>(values);
            OpType = type;
        }

        public GremlinHasOp(HasOpType type, Predicate predicate)
        {
            Predicate = predicate;
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
                case HasOpType.HasProperty:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey);
                    break;

                //has(key, value)
                case HasOpType.HasPropertyValue:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey, Value);
                    break;

                //has(key, predicate)
                case HasOpType.HasPropertyPredicate:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey, Predicate);
                    break;

                //has(label, key, value)
                case HasOpType.HasLabelPropertyValue:
                    inputContext.PivotVariable.Has(inputContext, Label, PropertyKey, Value);
                    break;

                //has(label, key, predicate)
                case HasOpType.HasLabelPropertyPredicate:
                    inputContext.PivotVariable.Has(inputContext, Label, PropertyKey, Predicate);
                    break;

                case HasOpType.HasPropertyTraversal:
                    Traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                    if (Traversal.GetStartOp() is GremlinParentContextOp)
                    {
                        if (Traversal.GremlinTranslationOpList.Count > 1)
                        {
                            Traversal.InsertGremlinOperator(1,  new GremlinValuesOp(PropertyKey));
                        }
                        else
                        {
                            Traversal.AddGremlinOperator(new GremlinValuesOp(PropertyKey));
                        }
                    }
                    GremlinToSqlContext hasContext = Traversal.GetEndOp().GetContext();
                    inputContext.PivotVariable.Has(inputContext, PropertyKey, hasContext);
                    break;

                //hasId(values)
                case HasOpType.HasId:
                    inputContext.PivotVariable.HasId(inputContext, Values);
                    break;

                //hasId(predicate)
                case HasOpType.HasIdPredicate:
                    inputContext.PivotVariable.HasId(inputContext, Predicate);
                    break;

                //hasKey(values)
                case HasOpType.HasKey:
                    inputContext.PivotVariable.HasKey(inputContext, Keys);
                    break;

                //hasKey(predicate)
                case HasOpType.HasKeyPredicate:
                    inputContext.PivotVariable.HasKey(inputContext, Predicate);
                    break;

                //hasLabel(values)
                case HasOpType.HasLabel:
                    inputContext.PivotVariable.HasLabel(inputContext, Values);
                    break;

                //hasLabel(predicate)
                case HasOpType.HasLabelPredicate:
                    inputContext.PivotVariable.HasLabel(inputContext, Predicate);
                    break;

                //hasValue(values)
                case HasOpType.HasValue:
                    inputContext.PivotVariable.HasValue(inputContext, Values);
                    break;

                //hasValue(predicate)
                case HasOpType.HasValuePredicate:
                    inputContext.PivotVariable.HasValue(inputContext, Predicate);
                    break;

            }

            return inputContext;
        }

    }
}
