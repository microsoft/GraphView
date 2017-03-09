using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal enum GremlinHasType
    {
        HasProperty,
        HasNotProperty,
        HasPropertyValueOrPredicate,
        HasPropertyTraversal,
        HasLabelPropertyValue,

        HasId,
        HasKey,
        HasLabel,
        HasValue,
    }
    internal class GremlinHasOp: GremlinTranslationOperator
    {
        public string PropertyKey { get; set; }
        public string Label { get; set; }
        public object ValueOrPredicate { get; set; }
        public List<object> ValuesOrPredicates { get; set; }
        public GraphTraversal2 Traversal { get; set; }
        public GremlinHasType Type { get; set; }

        public GremlinHasOp(GremlinHasType type, string propertyKey)
        {
            PropertyKey = propertyKey;
            Type = type;
        }

        public GremlinHasOp(string propertyKey, object valueOrPredicate)
        {
            PropertyKey = propertyKey;
            ValueOrPredicate = valueOrPredicate;
            Type = GremlinHasType.HasPropertyValueOrPredicate;
        }

        public GremlinHasOp(string propertyKey, GraphTraversal2 traversal)
        {
            PropertyKey = propertyKey;
            Traversal = traversal;
            Type = GremlinHasType.HasPropertyTraversal;
        }

        public GremlinHasOp(string label, string propertyKey, object value)
        {
            Label = label;
            PropertyKey = propertyKey;
            ValueOrPredicate = value;
            Type = GremlinHasType.HasLabelPropertyValue;
        }

        public GremlinHasOp(GremlinHasType type, params object[] valuesOrPredicates)
        {
            ValuesOrPredicates = new List<object>(valuesOrPredicates);
            Type = type;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            switch (Type)
            {
                //has(key)
                case GremlinHasType.HasProperty:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey);
                    break;

                //hasNot(key)
                case GremlinHasType.HasNotProperty:
                    inputContext.PivotVariable.HasNot(inputContext, PropertyKey);
                    break;

                //has(key, value) | has(key, predicate)
                case GremlinHasType.HasPropertyValueOrPredicate:
                    inputContext.PivotVariable.Has(inputContext, PropertyKey, ValueOrPredicate);
                    break;

                //has(key, traversal)
                case GremlinHasType.HasPropertyTraversal:
                    Traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                    if (Traversal.GetStartOp() is GremlinParentContextOp)
                    {
                        if (Traversal.GremlinTranslationOpList.Count > 1)
                        {
                            Traversal.InsertGremlinOperator(1, new GremlinValuesOp(PropertyKey));
                        }
                        else
                        {
                            Traversal.AddGremlinOperator(new GremlinValuesOp(PropertyKey));
                        }
                    }
                    GremlinToSqlContext hasContext = Traversal.GetEndOp().GetContext();
                    inputContext.AddPredicate(hasContext.ToSqlBoolean());
                    break;

                //has(label, key, value) | has(label, key, predicate)
                case GremlinHasType.HasLabelPropertyValue:
                    inputContext.PivotVariable.Has(inputContext, GremlinKeyword.Label, Label);
                    inputContext.PivotVariable.Has(inputContext, PropertyKey, ValueOrPredicate);
                    break;

                //hasId(values)
                case GremlinHasType.HasId:
                case GremlinHasType.HasLabel:
                    inputContext.PivotVariable.HasIdOrLabel(inputContext, Type, ValuesOrPredicates);
                    break;

                //===================================================================
                //hasKey(values) || hasValue(values)
                case GremlinHasType.HasKey:
                case GremlinHasType.HasValue:
                    inputContext.PivotVariable.HasKeyOrValue(inputContext, Type, ValuesOrPredicates);
                    break;
            }
            return inputContext;
        }

    }
}
