using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class Predicate
    {
        public object Value;
        public object Low;
        public object High;
        public List<object> Values;
        public PredicateType PredicateType;

        public Predicate(object value, PredicateType type)
        {
            Value = value;
            PredicateType = type;
        }

        public Predicate(int low, int high, PredicateType type)
        {
            Low = low;
            High = high;
            PredicateType = type;
        }

        public Predicate(PredicateType type, params object[] values)
        {
            Values = new List<object>();
            foreach(var value in values)
            {
                Values.Add(value);
            }
            PredicateType = type;
        }

        public static Predicate eq(object value)
        {
            return new Predicate(value, PredicateType.eq);
        }

        public static Predicate neq(object value)
        {
            return new Predicate(value, PredicateType.neq);
        }

        public static Predicate lt(int value)
        {
            return new Predicate(value, PredicateType.lt);
        }

        public static Predicate gt(int value)
        {
            return new Predicate(value, PredicateType.gt);
        }

        public static Predicate gte(int value)
        {
            return new Predicate(value, PredicateType.gte);
        }

        public static Predicate inside(int low, int high)
        {
            return new Predicate(low, high, PredicateType.inside);
        }

        public static Predicate outside(int low, int high)
        {
            return new Predicate(low, high, PredicateType.outside);
        }

        public static Predicate between(int low, int high)
        {
            return new Predicate(low, high, PredicateType.between);
        }

        public static Predicate within(params object[] objects)
        {
            return new Predicate(PredicateType.within, objects);
        }

        public static Predicate without(params object[] objects)
        {
            return new Predicate(PredicateType.without, objects);
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
