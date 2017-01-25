using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class FoldFunction : IAggregateFunction
    {
        List<FieldObject> buffer;
        ScalarFunction compose1;

        //public FoldFunction(ScalarFunction compose1)
        //{
        //    this.compose1 = compose1;
        //}

        public FoldFunction() { }

        public void Accumulate(params FieldObject[] values)
        {
            buffer.Add(values[0]);
        }

        public void Init()
        {
            buffer = new List<FieldObject>();
        }

        public FieldObject Terminate()
        {
            return new CollectionField(buffer);
        }
    }

    internal class CountFunction : IAggregateFunction
    {
        long count;

        public void Accumulate(params FieldObject[] values)
        {
            count++;
        }

        public void Init()
        {
            count = 0;
        }

        public FieldObject Terminate()
        {
            return new StringField(count.ToString());
        }
    }

    internal class TreeFunction : IAggregateFunction
    {
        private class TreeNode
        {
            private string _value;
            internal SortedDictionary<string, TreeNode> Children;

            internal TreeNode(string pValue)
            {
                _value = pValue;
                Children = new SortedDictionary<string, TreeNode>();
            }

            public override string ToString()
            {
                var strBuilder = new StringBuilder();
                strBuilder.Append("[");
                var cnt = 0;
                foreach (var child in Children)
                {
                    if (cnt++ != 0)
                        strBuilder.Append(",");
                    child.Value.ToString(strBuilder);
                }
                strBuilder.Append("]");
                return strBuilder.ToString();
            }

            private void ToString(StringBuilder strBuilder)
            {
                strBuilder.Append(_value).Append(":[");
                var cnt = 0;
                foreach (var child in Children)
                {
                    if (cnt++ != 0)
                        strBuilder.Append(",");
                    child.Value.ToString(strBuilder);
                }
                strBuilder.Append("]");
            }
        }

        private static void ConstructTree(TreeNode root, int index, CollectionField path)
        {
            if (index >= path.Collection.Count) return;
            var node = path.Collection[index++].ToString();

            TreeNode child;
            if (!root.Children.TryGetValue(node, out child))
            {
                child = new TreeNode(node);
                root.Children[node] = child;
            }

            ConstructTree(child, index, path);
        }

        private TreeNode _root;

        void IAggregateFunction.Accumulate(params FieldObject[] values)
        {
            if (values.Length != 1)
            {
                return;
            }

            ConstructTree(_root, 0, values[0] as CollectionField);
        }

        void IAggregateFunction.Init()
        {
            _root = new TreeNode("root");
        }

        FieldObject IAggregateFunction.Terminate()
        {
            return new StringField(_root.ToString());
        }
    }

    internal class StoreStateFunction : IAggregateFunction
    {
        List<FieldObject> aggregateState;

        public StoreStateFunction()
        {
            aggregateState = new List<FieldObject>();
        }

        public void Init()
        {
            aggregateState = new List<FieldObject>();
        }

        public void Accumulate(params FieldObject[] values)
        {
            aggregateState.Add(values[0]);
        }

        public FieldObject Terminate()
        {
            return new CollectionField(aggregateState);
        }
    }

    internal class GroupFunction : IAggregateFunction
    {
        Dictionary<FieldObject, List<RawRecord>> aggregateState;
        ScalarFunction aggregateTargetFunction;
        int elementPropertyProjectionIndex;

        public GroupFunction(ScalarFunction aggregateTargetFunction, int elementPropertyProjectionIndex)
        {
            aggregateState = new Dictionary<FieldObject, List<RawRecord>>();
            this.aggregateTargetFunction = aggregateTargetFunction;
            this.elementPropertyProjectionIndex = elementPropertyProjectionIndex;
        }

        public void Init()
        {
            aggregateState = new Dictionary<FieldObject, List<RawRecord>>();
        }

        public void Accumulate(params FieldObject[] values)
        {
            throw new NotImplementedException();
        }

        public void Accumulate(params Object[] values)
        {
            var groupByKey = values[0] as FieldObject;
            var groupByValue = values[1] as RawRecord;

            if (!aggregateState.ContainsKey(groupByKey))
            {
                aggregateState.Add(groupByKey, new List<RawRecord>());
            }

            aggregateState[groupByKey].Add(groupByValue);
        }

        public FieldObject Terminate()
        {
            Dictionary<FieldObject, FieldObject> resultCollection = new Dictionary<FieldObject, FieldObject>(aggregateState.Count);

            if (elementPropertyProjectionIndex >= 0)
            {
                foreach (FieldObject key in aggregateState.Keys)
                {
                    List<FieldObject> fo = new List<FieldObject>();
                    foreach (var rawRecord in aggregateState[key])
                    {
                        fo.Add(rawRecord[elementPropertyProjectionIndex]);
                    }
                    resultCollection[key] = new CollectionField(fo);
                }
            }
            else
            {
                foreach (FieldObject key in aggregateState.Keys)
                {
                    RawRecord rc = aggregateState[key][0];
                    FieldObject aggregateResult = aggregateTargetFunction.Evaluate(rc);
                    if (aggregateResult == null)
                    {
                        return null;
                    }
                    resultCollection[key] = aggregateResult;
                }
            }

            return new MapField(resultCollection);
        }
    }

    internal class CapAggregate : IAggregateFunction
    {
        List<Tuple<string, IAggregateFunction>> sideEffectStates;

        public CapAggregate()
        {
            sideEffectStates = new List<Tuple<string, IAggregateFunction>>();
        }

        public void AddCapatureSideEffectState(string key, IAggregateFunction sideEffect)
        {
            sideEffectStates.Add(new Tuple<string, IAggregateFunction>(key, sideEffect));
        }

        public void Accumulate(params FieldObject[] values)
        {
            return;
        }

        public void Init()
        {
            return;
        }

        public FieldObject Terminate()
        {
            var map = new Dictionary<FieldObject, FieldObject>();

            foreach (var tuple in sideEffectStates)
            {
                var key = tuple.Item1;
                var sideEffectState = tuple.Item2;
                var capResult = sideEffectState.Terminate();
                if (capResult != null)
                    map.Add(new StringField(key), capResult);
            }

            return new MapField(map);
        }
    }

    internal class GroupSideEffectOperator : GraphViewExecutionOperator
    {
        public GroupFunction GroupState { get; private set; }
        GraphViewExecutionOperator inputOp;
        ScalarFunction groupByKeyFunction;

        public GroupSideEffectOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            ScalarFunction aggregateTargetFunction,
            int elementPropertyProjectionIndex)
        {
            this.inputOp = inputOp;
            this.groupByKeyFunction = groupByKeyFunction;

            GroupState = new GroupFunction(aggregateTargetFunction, elementPropertyProjectionIndex);
            Open();
        }

        public override RawRecord Next()
        {
            if (inputOp.State())
            {
                RawRecord r = inputOp.Next();
                if (r == null)
                {
                    Close();
                    return null;
                }

                FieldObject groupByKey = groupByKeyFunction.Evaluate(r);

                GroupState.Accumulate(new Object[]{ groupByKey, r });

                if (!inputOp.State())
                {
                    Close();
                }
                return r;
            }

            return null;
        }

        public override void ResetState()
        {
            GroupState.Init();
            inputOp.ResetState();
            Open();
        }
    }

    internal class GroupOperator : GraphViewExecutionOperator
    {
        GraphViewExecutionOperator inputOp;
        ScalarFunction groupByKeyFunction;
        ScalarFunction aggregateTargetFunction;
        int elementPropertyProjectionIndex;

        Dictionary<FieldObject, List<RawRecord>> aggregatedState;

        public GroupOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            ScalarFunction aggregateTargetFunction,
            int elementPropertyProjectionIndex)
        {
            this.inputOp = inputOp;
            this.groupByKeyFunction = groupByKeyFunction;
            this.aggregateTargetFunction = aggregateTargetFunction;
            this.elementPropertyProjectionIndex = elementPropertyProjectionIndex;

            aggregatedState = new Dictionary<FieldObject, List<RawRecord>>();
            Open();
            //aggregatedState.Clear();
        }

        public override RawRecord Next()
        {
            RawRecord r = null;
            while (inputOp.State() && (r = inputOp.Next()) != null)
            {
                FieldObject groupByKey = groupByKeyFunction.Evaluate(r);
                if (!aggregatedState.ContainsKey(groupByKey))
                {
                    aggregatedState.Add(groupByKey, new List<RawRecord>());
                }
                aggregatedState[groupByKey].Add(r);
            }

            Dictionary<FieldObject, FieldObject> resultCollection = new Dictionary<FieldObject, FieldObject>(aggregatedState.Count);
            if (elementPropertyProjectionIndex >= 0)
            {
                foreach (FieldObject key in aggregatedState.Keys)
                {
                    List<FieldObject> fo = new List<FieldObject>();
                    foreach (var rawRecord in aggregatedState[key])
                    {
                        fo.Add(rawRecord[elementPropertyProjectionIndex]);
                    }
                    resultCollection[key] = new CollectionField(fo);
                }
            }
            else
            {
                foreach (FieldObject key in aggregatedState.Keys)
                {
                    RawRecord rc = aggregatedState[key][0];
                    FieldObject aggregateResult = aggregateTargetFunction.Evaluate(rc);
                    if (aggregateResult == null)
                    {
                        Close();
                        return null;
                    }
                    resultCollection[key] = aggregateResult;
                }
            }

            RawRecord resultRecord = new RawRecord();
            resultRecord.Append(new MapField(resultCollection));

            Close();
            return resultRecord;
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            aggregatedState.Clear();
            Open();
        }
    }
}
