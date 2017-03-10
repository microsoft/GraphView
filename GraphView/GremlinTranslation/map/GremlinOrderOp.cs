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
        public Dictionary<GraphTraversal2, IComparer> ByModulatingMap { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinOrderOp(GremlinKeyword.Scope scope)
        {
            ByModulatingMap = new Dictionary<GraphTraversal2, IComparer>();
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
                ByModulatingMap.Add(GraphTraversal2.__(), new IncrOrder());
            }

            var newByModulatingMap = new Dictionary<GremlinToSqlContext, IComparer>();
            foreach (var pair in ByModulatingMap)
            {
                pair.Key.GetStartOp().InheritedVariableFromParent(inputContext);

                newByModulatingMap.Add(pair.Key.GetEndOp().GetContext(), pair.Value);
            }

            inputContext.PivotVariable.Order(inputContext, newByModulatingMap, Scope);
            
            return inputContext;
        }

        public override void ModulateBy()
        {
            ByModulatingMap.Add(GraphTraversal2.__(), new IncrOrder());
        }

        public override void ModulateBy(GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingMap.Add(GraphTraversal2.__(), new IncrOrder());
                    break;
                case GremlinKeyword.Order.Desr:
                    ByModulatingMap.Add(GraphTraversal2.__(), new DecrOrder());
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingMap.Add(GraphTraversal2.__(), new ShuffleOrder());
                    break;
            }
        }

        public override void ModulateBy(IComparer comparer)
        {
            ByModulatingMap.Add(GraphTraversal2.__(), comparer);
        }

        public override void ModulateBy(string key)
        {
            ByModulatingMap.Add(GraphTraversal2.__().Values(key), new IncrOrder());
        }

        public override void ModulateBy(string key, IComparer comparer)
        {
            ByModulatingMap.Add(GraphTraversal2.__().Values(key), comparer);
        }

        public override void ModulateBy(string key, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingMap.Add(GraphTraversal2.__().Values(key), new IncrOrder());
                    break;
                case GremlinKeyword.Order.Desr:
                    ByModulatingMap.Add(GraphTraversal2.__().Values(key), new DecrOrder());
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingMap.Add(GraphTraversal2.__().Values(key), new ShuffleOrder());
                    break;
            }
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByModulatingMap.Add(traversal, new IncrOrder());
        }

        public override void ModulateBy(GraphTraversal2 traversal, IComparer comparer)
        {
            ByModulatingMap.Add(traversal, comparer);
        }

        public override void ModulateBy(GraphTraversal2 traversal, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingMap.Add(traversal, new IncrOrder());
                    break;
                case GremlinKeyword.Order.Desr:
                    ByModulatingMap.Add(traversal, new DecrOrder());
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingMap.Add(traversal, new ShuffleOrder());
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
