using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class Predicate
    {
        public bool IsAliasValue;
        public object Value;
        public object Low;
        public object High;
        public List<object> Values;
        public PredicateType PredicateType;

        public Predicate(PredicateType type, object value, bool isAliasValue = false)
        {
            Value = value;
            PredicateType = type;
            IsAliasValue = isAliasValue;
        }

        public Predicate(PredicateType type, int low, int high, bool isAliasValue = false)
        {
            Low = low;
            High = high;
            PredicateType = type;
            IsAliasValue = isAliasValue;
        }

        public Predicate(PredicateType type, bool isAliasValue = false, params object[] values)
        {
            Values = new List<object>();
            foreach(var value in values)
            {
                Values.Add(value);
            }
            PredicateType = type;
            IsAliasValue = isAliasValue;
        }

        public static Predicate eq(object value, bool isAliasValue = false)
        {
            return new Predicate(PredicateType.eq, value, isAliasValue);
        }

        public static Predicate neq(object value, bool isAliasValue = false)
        {
            return new Predicate(PredicateType.eq, value, isAliasValue);
        }

        public static Predicate lt(int value)
        {
            return new Predicate(PredicateType.eq, value);
        }

        public static Predicate gt(int value)
        {
            return new Predicate(PredicateType.eq, value);
        }

        public static Predicate gte(int value)
        {
            return new Predicate(PredicateType.eq, value);
        }

        public static Predicate inside(int low, int high)
        {
            return new Predicate(PredicateType.inside, low, high);
        }

        public static Predicate outside(int low, int high)
        {
            return new Predicate(PredicateType.outside, low, high);
        }

        public static Predicate between(int low, int high)
        {
            return new Predicate(PredicateType.between, low, high);
        }

        public static Predicate within(params object[] objects, bool isAliasValue = false)
        {
            return new Predicate(PredicateType.within, objects, isAliasValue);
        }

        public static Predicate without(params object[] objects, bool isAliasValue = false)
        {
            return new Predicate(PredicateType.without, objects, isAliasValue);
        }
    }

    internal enum PredicateType
    {
        eq,
        neq,
        lt,
        lte,
        gt,
        gte,
        inside,
        outside,
        between,
        within,
        without
    }
}
