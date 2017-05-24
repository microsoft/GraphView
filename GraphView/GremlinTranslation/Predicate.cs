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
        public object Low { get; set; }
        public object High { get; set; }
        public PredicateType PredicateType { get; set; }

        public Predicate(PredicateType type, object value)
        {
            Value = value;
            PredicateType = type;
        }

        public Predicate(PredicateType type, object low, object high)
        {
            Low = low;
            High = high;
            PredicateType = type;
        }

        public Predicate(PredicateType type, params object[] values)
        {
            Values = new List<object>();
            if (values != null)
            {
                foreach (var value in values)
                {
                    Values.Add(value);
                }
            }
            PredicateType = type;
        }

        public Predicate And(Predicate predicate)
        {
            return new AndPredicate(this, predicate);
        }

        public Predicate Or(Predicate predicate)
        {
            return new OrPredicate(this, predicate);
        }

        public static Predicate eq(object value)
        {
            return new Predicate(PredicateType.eq, value);
        }

        public static Predicate neq(object obj)
        {
            return new Predicate(PredicateType.neq, obj);
        }

        public static Predicate lt(object value)
        {
            return new Predicate(PredicateType.lt, value);
        }

        public static Predicate lte(object value)
        {
            return new Predicate(PredicateType.lte, value);
        }

        public static Predicate gt(object value)
        {
            return new Predicate(PredicateType.gt, value);
        }

        public static Predicate gte(object value)
        {
            return new Predicate(PredicateType.gte, value);
        }

        public static Predicate inside(object low, object high)
        {
            return new Predicate(PredicateType.inside, low, high);
        }

        public static Predicate outside(object low, object high)
        {
            return new Predicate(PredicateType.outside, low, high);
        }

        public static Predicate between(object low, object high)
        {
            return new Predicate(PredicateType.between, low, high);
        }

        public static Predicate within(params object[] objects)
        {
            return new Predicate(PredicateType.within, objects);
        }

        public static Predicate within(object label)
        {
            return new Predicate(PredicateType.within, label);
        }

        public static Predicate without(params object[] objects)
        {
            return new Predicate(PredicateType.without, objects);
        }

        public static Predicate without(object label)
        {
            return new Predicate(PredicateType.without, label);
        }

        public static Predicate not(Predicate predicate)
        {
            if (predicate is AndPredicate)
            {
                // (a * b)' = a' + b'
                var andPredicate = predicate as AndPredicate;
                List<Predicate> predicates = new List<Predicate>();
                foreach (var p in andPredicate.PredicateList)
                {
                    predicates.Add(not(p));
                }
                return new OrPredicate(predicates.ToArray());
            }
            if (predicate is OrPredicate)
            {
                // (a + b)' = a' * b'
                var orPredicate = predicate as OrPredicate;
                List<Predicate> predicates = new List<Predicate>();
                foreach (var p in orPredicate.PredicateList)
                {
                    predicates.Add(not(p));
                }
                return new AndPredicate(predicates.ToArray());
            }
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
                predicate.PredicateType = PredicateType.lteOrgte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.lteOrgte)
            {
                predicate.PredicateType = PredicateType.inside;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.outside)
            {
                predicate.PredicateType = PredicateType.gteAndlte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.gteAndlte)
            {
                predicate.PredicateType = PredicateType.outside;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.between)
            {
                predicate.PredicateType = PredicateType.ltOrgte;
                return predicate;
            }
            if (predicate.PredicateType == PredicateType.ltOrgte)
            {
                predicate.PredicateType = PredicateType.between;
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

    public class AndPredicate: Predicate
    {
        public List<Predicate> PredicateList { get; set; }

        public AndPredicate(params Predicate[] predicates): base(PredicateType.and, null)
        {
            PredicateList = new List<Predicate>(predicates);
        }
    }

    public class OrPredicate : Predicate
    {
        public List<Predicate> PredicateList { get; set; }

        public OrPredicate(params Predicate[] predicates): base(PredicateType.or, null)
        {
            PredicateList = new List<Predicate>(predicates);
        }
    }

    public enum PredicateType
    {
        and,
        or,

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

        lteOrgte,
        gteAndlte,
        ltOrgte
    }
}
