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
        public List<Tuple<object, IComparer>> ByModulatingMap { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinOrderOp(GremlinKeyword.Scope scope)
        {
            ByModulatingMap = new List<Tuple<object, IComparer>>();
            Scope = scope;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (ByModulatingMap.Count == 0)
            {
                if (Scope == GremlinKeyword.Scope.global)
                    ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__(), new IncrOrder()));
                else
                    ByModulatingMap.Add(new Tuple<object, IComparer>("", new IncrOrder()));
            }

            var newByModulatingMap = new List<Tuple<object, IComparer>>();
            foreach (var pair in ByModulatingMap)
            {
                if (pair.Item1 is GraphTraversal2)
                {
                    ((GraphTraversal2)pair.Item1).GetStartOp().InheritedVariableFromParent(inputContext);
                    newByModulatingMap.Add(new Tuple<object, IComparer>(((GraphTraversal2)pair.Item1).GetEndOp().GetContext(), pair.Item2));
                }
                else if (pair.Item1 is GremlinKeyword.Column || pair.Item1 == "")
                {
                    newByModulatingMap.Add(new Tuple<object, IComparer>(pair.Item1, pair.Item2));
                }
                else
                {
                    throw new ArgumentException();
                }
            }

            inputContext.PivotVariable.Order(inputContext, newByModulatingMap, Scope);
            
            return inputContext;
        }

        public override void ModulateBy()
        {
            if (Scope == GremlinKeyword.Scope.global)
                ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__(), new IncrOrder()));
            else
                ByModulatingMap.Add(new Tuple<object, IComparer>("", new IncrOrder()));
        }

        public override void ModulateBy(GremlinKeyword.Order order)
        {
            object key;
            if (Scope == GremlinKeyword.Scope.global)
                key = GraphTraversal2.__();
            else
                key = "";
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(key, new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(key, new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(key, new ShuffleOrder()));
                    break;
            }
        }

        public override void ModulateBy(IComparer comparer)
        {
            if (Scope == GremlinKeyword.Scope.global)
                ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__(), comparer));
            else
                ByModulatingMap.Add(new Tuple<object, IComparer>("", comparer));
        }

        public override void ModulateBy(string key)
        {
            ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__().Values(key), new IncrOrder()));
        }

        public override void ModulateBy(string key, IComparer comparer)
        {
            ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__().Values(key), comparer));
        }

        public override void ModulateBy(string key, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__().Values(key), new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__().Values(key), new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(GraphTraversal2.__().Values(key), new ShuffleOrder()));
                    break;
            }
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByModulatingMap.Add(new Tuple<object, IComparer>(traversal, new IncrOrder()));
        }

        public override void ModulateBy(GraphTraversal2 traversal, IComparer comparer)
        {
            ByModulatingMap.Add(new Tuple<object, IComparer>(traversal, comparer));
        }

        public override void ModulateBy(GraphTraversal2 traversal, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(traversal, new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(traversal, new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(traversal, new ShuffleOrder()));
                    break;
            }
        }

        public override void ModulateBy(GremlinKeyword.Column column)
        {
            ByModulatingMap.Add(new Tuple<object, IComparer>(column, new IncrOrder()));
        }

        public override void ModulateBy(GremlinKeyword.Column column, IComparer comparer)
        {
            ByModulatingMap.Add(new Tuple<object, IComparer>(column, comparer));
        }

        public override void ModulateBy(GremlinKeyword.Column column, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(column, new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(column, new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingMap.Add(new Tuple<object, IComparer>(column, new ShuffleOrder()));
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
