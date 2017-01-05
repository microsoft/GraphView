using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class AggregateFunction : GraphViewExecutionOperator
    {
        internal GraphViewExecutionOperator InputOperator;
        protected List<int> GroupByFieldsList;
        protected Queue<RawRecord> InputBuffer;
        protected Queue<RawRecord> OutputBuffer;

        internal AggregateFunction(GraphViewExecutionOperator pInputOperator, List<int> pGroupByFieldsList)
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

    internal class CountOperator : AggregateFunction
    {
        internal CountOperator(GraphViewExecutionOperator pInputOperatr, List<int> pGroupByFieldsList) 
            : base(pInputOperatr, pGroupByFieldsList)
        { }

        internal override RawRecord ApplyAggregateFunction(List<RawRecord> groupedRawRecords)
        {
            var result = new RawRecord(1);
            result.fieldValues[0] = groupedRawRecords.Count.ToString();
            return result;
        }
    }

    internal class FoldOperator : AggregateFunction
    {
        internal int FoldedFieldIdx;

        internal FoldOperator(GraphViewExecutionOperator pInputOperatr, List<int> pGroupByFieldsList, int pFoldedFieldIdx)
            : base(pInputOperatr, pGroupByFieldsList)
        {
            FoldedFieldIdx = pFoldedFieldIdx;
        }

        internal override RawRecord ApplyAggregateFunction(List<RawRecord> groupedRawRecords)
        {
            var result = new RawRecord(2);
            var foldedList = new StringBuilder("[");
            var foldedListMetaInfo = new StringBuilder();

            foreach (var record in groupedRawRecords)
            {
                var value = record.fieldValues[FoldedFieldIdx];
                foldedList.Append(value).Append(",");
                foldedListMetaInfo.Append(value.Length).Append(",");
            }

            if (foldedListMetaInfo.Length != 0)
            {
                foldedList.Remove(foldedList.Length - 1, 1);
                foldedListMetaInfo.Remove(foldedListMetaInfo.Length - 1, 1);
            }
            foldedList.Append("]");

            result.fieldValues[0] = foldedList.ToString();
            result.fieldValues[1] = foldedListMetaInfo.ToString();

            return result;
        }
    }

    internal class DeduplicateOperator : AggregateFunction
    {
        internal DeduplicateOperator(GraphViewExecutionOperator pInputOperatr, List<int> pGroupByFieldsList)
            : base(pInputOperatr, pGroupByFieldsList)
        { }

        internal override RawRecord ApplyAggregateFunction(List<RawRecord> groupedRawRecords)
        {
            return groupedRawRecords.First();
        }
    }

    internal class TreeOperator : AggregateFunction
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
                var path = t.fieldValues[pathIndex];
                ConstructTree(ref root, ref path);
            }

            var result = new RawRecord(1);
            result.fieldValues[0] = root.ToString();
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
}
