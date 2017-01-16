using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GroupByFunction : GraphViewExecutionOperator
    {
        internal GraphViewExecutionOperator InputOperator;
        // TODO: Wrong design, group by should be an independent operator
        protected List<int> GroupByFieldsList;
        protected Queue<RawRecord> InputBuffer;
        protected Queue<RawRecord> OutputBuffer;

        internal GroupByFunction(GraphViewExecutionOperator pInputOperator, List<int> pGroupByFieldsList)
        {
            InputOperator = pInputOperator;
            GroupByFieldsList = pGroupByFieldsList;
            InputBuffer = new Queue<RawRecord>();
            OutputBuffer = new Queue<RawRecord>();
            this.Open();
        }

        private static List<List<RawRecord>> GroupBySingleColumn(List<RawRecord> input, int fieldIdx)
        {
            return input.GroupBy(i => i.fieldValues[fieldIdx]).Select(r => r.ToList()).ToList();
        }

        private static List<List<RawRecord>> GroupBySingleColumn(List<List<RawRecord>> input, int fieldIdx)
        {
            var result = new List<List<RawRecord>>();

            foreach (var test in input)
                result.AddRange(GroupBySingleColumn(test, fieldIdx));

            return result;
        }

        internal static List<List<RawRecord>> GroupBy(Queue<RawRecord> input, List<int> fieldIdxList)
        {
            var result = new List<List<RawRecord>> {input.ToList()};

            foreach (var i in fieldIdxList)
                result = GroupBySingleColumn(result, i);

            return result;
        }

        internal abstract RawRecord ApplyAggregateFunction(List<RawRecord> groupedRawRecords);

        public override RawRecord Next()
        {
            if (!State()) return null;
            // If the output buffer is not empty, returns a result.
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }

            // Fills the input buffer by pulling from the input operator
            while (InputOperator.State())
            {
                if (InputOperator != null && InputOperator.State())
                {
                    RawRecord result = InputOperator.Next();
                    if (result == null)
                    {
                        InputOperator.Close();
                        break;
                    }
                    else
                    {
                        InputBuffer.Enqueue(result);
                    }
                }
            }

            var groupedResults = GroupBy(InputBuffer, GroupByFieldsList);

            foreach (var groupedResult in groupedResults)
                OutputBuffer.Enqueue(ApplyAggregateFunction(groupedResult));

            InputBuffer.Clear();
            if (OutputBuffer.Count <= 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }
    }

    internal class FoldFunction : IAggregateFunction
    {
        List<FieldObject> buffer;

        public void Accumulate(params FieldObject[] values)
        {
            if (values.Length != 1)
            {
                return;
            }

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

    internal class TreeOperator : GroupByFunction
    {
        //TODO: tree().by()
        private class TreeNode
        {
            internal string value;
            internal SortedDictionary<string, TreeNode> children;

            internal TreeNode(string pValue)
            {
                value = pValue;
                children = new SortedDictionary<string, TreeNode>();
            }

            public override string ToString()
            {
                var strBuilder = new StringBuilder();
                strBuilder.Append("[");
                var cnt = 0;
                foreach (var child in children)
                {
                    if (cnt++ != 0)
                        strBuilder.Append(",");
                    child.Value.ToString(ref strBuilder);
                }
                strBuilder.Append("]");
                return strBuilder.ToString();
            }

            private void ToString(ref StringBuilder strBuilder)
            {
                strBuilder.Append(value).Append(":[");
                var cnt = 0;
                foreach (var child in children)
                {
                    if (cnt++ != 0)
                        strBuilder.Append(",");
                    child.Value.ToString(ref strBuilder);
                }
                strBuilder.Append("]");
            }
        }

        private int pathIndex;

        internal TreeOperator(GraphViewExecutionOperator pInputOperatr, List<int> pGroupByFieldsList, int pPathIndex)
            : base(pInputOperatr, pGroupByFieldsList)
        {
            pathIndex = pPathIndex;
        }

        internal override RawRecord ApplyAggregateFunction(List<RawRecord> groupedRawRecords)
        {
            var root = new TreeNode("root");
            foreach (RawRecord t in groupedRawRecords)
            {
                var path = t.fieldValues[pathIndex].ToString();
                ConstructTree(ref root, ref path);
            }

            var result = new RawRecord(1);
            result.fieldValues[0] = new StringField(root.ToString());
            return result;
        }

        private static string ExtractNode(ref string path)
        {
            var delimiter = "-->";
            var idx = path.IndexOf(delimiter, StringComparison.CurrentCultureIgnoreCase);
            if (idx == -1) return null;
            var node = path.Substring(0, idx);
            path = path.Substring(idx + delimiter.Length);
            return node;
        }

        private static void ConstructTree(ref TreeNode root, ref string path)
        {
            var node = ExtractNode(ref path);
            if (!string.IsNullOrEmpty(node))
            {
                TreeNode child;
                if (!root.children.TryGetValue(node, out child))
                {
                    child = new TreeNode(node);
                    root.children[node] = child;
                }

                ConstructTree(ref child, ref path);
            }
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
                map.Add(new StringField(key), sideEffectState.Terminate());
            }

            return new MapField(map);
        }
    }

    internal class GroupOperator : GraphViewExecutionOperator
    {
        GraphViewExecutionOperator inputOp;
        ScalarFunction groupByKeyFunction;
        ScalarFunction aggregateTargetFunction;

        Dictionary<FieldObject, FoldFunction> aggregatedState;

        public GroupOperator(
            GraphViewExecutionOperator inputOp,
            ScalarFunction groupByKeyFunction,
            ScalarFunction aggregateTargetFunction)
        {
            this.inputOp = inputOp;
            this.groupByKeyFunction = groupByKeyFunction;
            this.aggregateTargetFunction = aggregateTargetFunction;

            aggregatedState = new Dictionary<FieldObject, FoldFunction>();
            aggregatedState.Clear();
        }

        public override RawRecord Next()
        {
            RawRecord r = null;
            while (inputOp.State() && (r = inputOp.Next()) != null)
            {
                FieldObject groupByKey = groupByKeyFunction.Evaluate(r);
                if (!aggregatedState.ContainsKey(groupByKey))
                {
                    aggregatedState.Add(groupByKey, new FoldFunction());
                    aggregatedState[groupByKey].Init();
                }
                aggregatedState[groupByKey].Accumulate(aggregateTargetFunction.Evaluate(r));
            }

            Dictionary<FieldObject, FieldObject> resultCollection = new Dictionary<FieldObject, FieldObject>(aggregatedState.Count);
            foreach (FieldObject key in aggregatedState.Keys)
            {
                resultCollection[key] = aggregatedState[key].Terminate();
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
        }
    }
}
