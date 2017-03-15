using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal class GremlinOrderOp: GremlinTranslationOperator
    {
        public List<Tuple<GraphTraversal2, IComparer>> ByModulatingList { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinOrderOp(GremlinKeyword.Scope scope)
        {
            ByModulatingList = new List<Tuple<GraphTraversal2, IComparer>>();
            Scope = scope;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (ByModulatingList.Count == 0)
            {
                if (Scope == GremlinKeyword.Scope.global)
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__(), new IncrOrder()));
                else
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(null, new IncrOrder()));
            }

            var newByModulatingMap = new List<Tuple<GremlinToSqlContext, IComparer>>();
            foreach (var pair in ByModulatingList)
            {
                GraphTraversal2 traversal = pair.Item1;
                if (traversal == null)
                {
                    newByModulatingMap.Add(new Tuple<GremlinToSqlContext, IComparer>(null, pair.Item2));
                }
                else
                {
                    GremlinToSqlContext context = null;

                    if (Scope == GremlinKeyword.Scope.global)
                    {
                        traversal.GetStartOp().InheritedVariableFromParent(inputContext);
                        context = traversal.GetEndOp().GetContext();
                    }

                    if (Scope == GremlinKeyword.Scope.local)
                    {
                        //g.V().groupCount().order(local).by(keys) or g.V().groupCount().order(local).by(__.select(keys))
                        if (traversal.GremlinTranslationOpList.Count >= 2 && traversal.GremlinTranslationOpList[1] is GremlinSelectColumnOp)
                        {
                            //FROM selectColumn(C._value, "Keys"/"Values")
                            GremlinToSqlContext newContext = new GremlinToSqlContext();
                            GremlinOrderLocalInitVariable initVar = new GremlinOrderLocalInitVariable();
                            newContext.VariableList.Add(initVar);
                            newContext.SetPivotVariable(initVar);

                            traversal.GetStartOp().InheritedContextFromParent(newContext);
                            context = traversal.GetEndOp().GetContext();
                        }
                        else
                        {
                            //FROM decompose1(C._value)
                            GremlinToSqlContext newContext = new GremlinToSqlContext();
                            GremlinDecompose1Variable decompose1 = new GremlinDecompose1Variable(inputContext.PivotVariable);
                            newContext.VariableList.Add(decompose1);
                            newContext.TableReferences.Add(decompose1);
                            newContext.SetPivotVariable(decompose1);

                            traversal.GetStartOp().InheritedContextFromParent(newContext);
                            context = traversal.GetEndOp().GetContext();
                        }
                    }

                    newByModulatingMap.Add(new Tuple<GremlinToSqlContext, IComparer>(context, pair.Item2));
                }
            }

            inputContext.PivotVariable.Order(inputContext, newByModulatingMap, Scope);

            return inputContext;
        }

        public override void ModulateBy()
        {
            if (Scope == GremlinKeyword.Scope.global)
                ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__(), new IncrOrder()));
            else
                ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(null, new IncrOrder()));
        }

        public override void ModulateBy(GremlinKeyword.Order order)
        {
            GraphTraversal2 key;
            if (Scope == GremlinKeyword.Scope.global)
                key = GraphTraversal2.__();
            else
                key = null;
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(key, new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(key, new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(key, new ShuffleOrder()));
                    break;
            }
        }

        public override void ModulateBy(IComparer comparer)
        {
            if (Scope == GremlinKeyword.Scope.global)
                ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__(), comparer));
            else
                ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(null, comparer));
        }

        public override void ModulateBy(string key)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Values(key), new IncrOrder()));
        }

        public override void ModulateBy(string key, IComparer comparer)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Values(key), comparer));
        }

        public override void ModulateBy(string key, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Values(key), new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Values(key), new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Values(key), new ShuffleOrder()));
                    break;
            }
        }

        public override void ModulateBy(GraphTraversal2 traversal)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(traversal, new IncrOrder()));
        }

        public override void ModulateBy(GraphTraversal2 traversal, IComparer comparer)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(traversal, comparer));
        }

        public override void ModulateBy(GraphTraversal2 traversal, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(traversal, new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(traversal, new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(traversal, new ShuffleOrder()));
                    break;
            }
        }

        public override void ModulateBy(GremlinKeyword.Column column)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Select(column), new IncrOrder()));
        }

        public override void ModulateBy(GremlinKeyword.Column column, IComparer comparer)
        {
            ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Select(column), comparer));
        }

        public override void ModulateBy(GremlinKeyword.Column column, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Select(column), new IncrOrder()));
                    break;
                case GremlinKeyword.Order.Decr:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Select(column), new DecrOrder()));
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ByModulatingList.Add(new Tuple<GraphTraversal2, IComparer>(GraphTraversal2.__().Select(column), new ShuffleOrder()));
                    break;
            }
        }

        public class IncrOrder : IComparer
        {
            public int Compare(object x, object y)
            {
                if (ReferenceEquals(x, y)) return 0;
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
                if (ReferenceEquals(x, y)) return 0;
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
