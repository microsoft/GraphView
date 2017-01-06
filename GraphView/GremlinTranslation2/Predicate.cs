using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    public class Predicate
    {
        public string Label { get; set; }
        public long Number { get; set; }
        public List<object> Values { get; set; }
        public long Low { get; set; }
        public long High { get; set; }
        public PredicateType PredicateType { get; set; }

        public Predicate(PredicateType type, string label)
        {
            Label = label;
            PredicateType = type;
        }
        public Predicate(PredicateType type, long number)
        {
            Number = number;
            PredicateType = type;
        }


        public Predicate(PredicateType type, int low, int high)
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

        public static Predicate eq(string value)
        {
            return new Predicate(PredicateType.eq, value);
        }

        public static Predicate eq(long number)
        {
            return new Predicate(PredicateType.eq, number);
        }

        public static Predicate neq(object obj)
        {
            return new Predicate(PredicateType.neq, obj);
        }

        public static Predicate neq(string label)
        {
            return new Predicate(PredicateType.neq, label);
        }

        public static Predicate neq(long number)
        {
            return new Predicate(PredicateType.neq, number);
        }

        public static Predicate lt(long value)
        {
            return new Predicate(PredicateType.lt, value);
        }

        public static Predicate gt(long value)
        {
            return new Predicate(PredicateType.gt, value);
        }

        public static Predicate gte(long value)
        {
            return new Predicate(PredicateType.gte, value);
        }

        public static Predicate inside(long low, long high)
        {
            return new Predicate(PredicateType.inside, low, high);
        }

        public static Predicate outside(long low, long high)
        {
            return new Predicate(PredicateType.outside, low, high);
        }

        public static Predicate between(long low, long high)
        {
            return new Predicate(PredicateType.between, low, high);
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

    public enum PredicateType
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
