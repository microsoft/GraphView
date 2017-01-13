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

        void IAggregateFunction.Accumulate(params FieldObject[] values)
        {
            if (values.Length != 1)
            {
                return;
            }

            buffer.Add(values[0]);
        }

        void IAggregateFunction.Init()
        {
            buffer = new List<FieldObject>();
        }

        FieldObject IAggregateFunction.Terminate()
        {
            return new CollectionField(buffer);
        }
    }

    internal class CountFunction : IAggregateFunction
    {
        long count;

        void IAggregateFunction.Accumulate(params FieldObject[] values)
        {
            count++;
        }

        void IAggregateFunction.Init()
        {
            count = 0;
        }

        FieldObject IAggregateFunction.Terminate()
        {
            return new StringField(count.ToString());
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
        Dictionary<FieldObject, int> aggregateState;    // To consider: whether a single key is enough, or a composite key?

        public StoreStateFunction(int stateIndex)
        {
            this.stateIndex = stateIndex;
        }

        void Init()
        {
            aggregateState = new Dictionary<FieldObject, int>();
        }

        void Accumulate(params FieldObject[] values)
        {
            if (aggregateState.ContainsKey(values[0]))
            {
                aggregateState[values[0]]++;
            }
            else
            {
                aggregateState.Add(values[0], 1);
            }
        }

        FieldObject Terminate()
        {
            StringBuilder sb = new StringBuilder();

            foreach (FieldObject key in aggregateState.Keys)
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }

                sb.AppendFormat("{0}:{1}", key.ToString(), aggregateState[key]);
            }

            return new StringField("[" + sb.ToString() + "]");
        }
    }
}
