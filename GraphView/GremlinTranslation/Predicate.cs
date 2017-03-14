using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    public class Predicate
    {
        public bool IsTag { get; set; }
        public object Value { get; set; }
        public List<object> Values { get; set; }
        public double Low { get; set; }
        public double High { get; set; }
        public PredicateType PredicateType { get; set; }

        public Predicate(PredicateType type, object value)
        {
            Value = value;
            PredicateType = type;
        }

        public Predicate(PredicateType type, double low, double high)
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

        public static Predicate eq(double number)
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

        public static Predicate neq(double number)
        {
            return new Predicate(PredicateType.neq, number);
        }

        public static Predicate lt(double value)
        {
            return new Predicate(PredicateType.lt, value);
        }

        public static Predicate lte(double value)
        {
            return new Predicate(PredicateType.lte, value);
        }

        public static Predicate gt(double value)
        {
            return new Predicate(PredicateType.gt, value);
        }
        // To support case: __.where('c',gt('u'). c and u represent 2 columns.
        public static Predicate gt(String value)
        {
            return new Predicate(PredicateType.gt, value);
        }

        public static Predicate gte(double value)
        {
            return new Predicate(PredicateType.gte, value);
        }

        public static Predicate inside(double low, double high)
        {
            return new Predicate(PredicateType.inside, low, high);
        }

        public static Predicate outside(double low, double high)
        {
            return new Predicate(PredicateType.outside, low, high);
        }

        public static Predicate between(double low, double high)
        {
            return new Predicate(PredicateType.between, low, high);
        }

        public static Predicate within(params object[] objects)
        {
            return new Predicate(PredicateType.within, objects);
        }

        public static Predicate within(string label)
        {
            return new Predicate(PredicateType.within, label);
        }

        public static Predicate without(params object[] objects)
        {
            return new Predicate(PredicateType.without, objects);
        }

        public static Predicate without(string label)
        {
            return new Predicate(PredicateType.without, label);
        }

        public static Predicate not(Predicate predicate)
        {
            if (predicate.PredicateType == PredicateType.eq)
            {
                predicate.PredicateType = PredicateType.neq;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.neq)
            {
                predicate.PredicateType = PredicateType.eq;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.lt)
            {
                predicate.PredicateType = PredicateType.gte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.lte)
            {
                predicate.PredicateType = PredicateType.gt;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.gt)
            {
                predicate.PredicateType = PredicateType.lte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.gte)
            {
                predicate.PredicateType = PredicateType.lt;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.inside)
            {
                predicate.PredicateType = PredicateType.lteAndgte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.outside)
            {
                predicate.PredicateType = PredicateType.gteAndlte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.between)
            {
                predicate.PredicateType = PredicateType.ltAndgte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.within)
            {
                predicate.PredicateType = PredicateType.without;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.without)
            {
                predicate.PredicateType = PredicateType.within;
                return predicate;
            }
            throw new QueryCompilationException();
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
        without,

        lteAndgte,
        gteAndlte,
        ltAndgte
    }
}
