using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOrderOp: GremlinTranslationOperator
    {
        public List<IComparer> OrderComparer { get; set; }
        public List<object> ByList { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinOrderOp(GremlinKeyword.Scope scope)
        {
            ByList = new List<object>();
            OrderComparer = new List<IComparer>();
            Scope = scope;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<object> byList = new List<object>();
            foreach (var item in ByList)
            {
                if (item is GraphTraversal2)
                {
                    (item as GraphTraversal2).GetStartOp().InheritedVariableFromParent(inputContext);
                    byList.Add((item as GraphTraversal2).GetEndOp().GetContext());
                }
                else if (item == null)
                {
                    byList.Add(inputContext.PivotVariable.DefaultProjection());
                }
                else if (item is string)
                {
                    byList.Add(item);
                }
                else
                {
                    throw new QueryCompilationException();
                }
            }

            if (!ByList.Any())
            {
                byList.Add(inputContext.PivotVariable.DefaultProjection());
                OrderComparer.Add(new IncrOrder());
            }

            inputContext.PivotVariable.Order(inputContext, byList, OrderComparer, Scope);
            
            return inputContext;
        }

        public override void ModulateBy()
        {
            ByList.Add(null);
            OrderComparer.Add(new IncrOrder());
        }

        public override void ModulateBy(string key)
        {
            ByList.Add(key);
            OrderComparer.Add(new IncrOrder());
        }

        public override void ModulateBy(string key, IComparer comparer)
        {
            ByList.Add(key);
            OrderComparer.Add(comparer);
        }

        public override void ModulateBy(string key, GremlinKeyword.Order order)
        {
            ByList.Add(key);
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    OrderComparer.Add(new IncrOrder());
                    break;
                case GremlinKeyword.Order.Desr:
                    OrderComparer.Add(new DecrOrder());
                    break;
                case GremlinKeyword.Order.Shuffle:
                    OrderComparer.Add(new ShuffleOrder());
                    break;
            }
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByList.Add(traversal);
            OrderComparer.Add(new IncrOrder());
        }

        public override void ModulateBy(GraphTraversal2 traversal, IComparer comparer)
        {
            ByList.Add(traversal);
            OrderComparer.Add(comparer);
        }

        public override void ModulateBy(GraphTraversal2 traversal, GremlinKeyword.Order order)
        {
            ByList.Add(traversal);
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    OrderComparer.Add(new IncrOrder());
                    break;
                case GremlinKeyword.Order.Desr:
                    OrderComparer.Add(new DecrOrder());
                    break;
                case GremlinKeyword.Order.Shuffle:
                    OrderComparer.Add(new ShuffleOrder());
                    break;
            }
        }

        public class IncrOrder : IComparer
        {
            public int Compare(object x, object y)
            {
                if (x is string)
                    return ((IComparable) x).CompareTo((IComparable) y);
                else
                    return ((IComparable) Convert.ToDouble(x)).CompareTo(Convert.ToDouble(y));
            }
        }

        public class DecrOrder : IComparer
        {
            public int Compare(object x, object y)
            {
                if (x is string)
                    return ((IComparable)y).CompareTo((IComparable)x);
                else
                    return ((IComparable)Convert.ToDouble(y)).CompareTo(Convert.ToDouble(x));
            }
        }

        public class ShuffleOrder : IComparer
        {
            private Random random = new Random();
            public int Compare(object x, object y)
            {
                return random.NextDouble() > 0.5 ? 1 : -1;
            }
        }
    }
}
