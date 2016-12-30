using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class TVFOperator : GraphViewExecutionOperator
    {
        internal GraphViewExecutionOperator InputOperator;
        protected Queue<RawRecord> InputBuffer;
        protected Queue<RawRecord> OutputBuffer;
        protected int InputBufferSize;
        protected int OutputBufferSize;
        protected int NewFieldIdx;

        internal TVFOperator(GraphViewExecutionOperator pInputOperator, int pNewFieldIdx, int pInputBufferSize,
            int pOutputBufferSize)
        {
            InputOperator = pInputOperator;
            NewFieldIdx = pNewFieldIdx;
            InputBuffer = new Queue<RawRecord>();
            OutputBuffer = new Queue<RawRecord>();
            InputBufferSize = pInputBufferSize;
            OutputBufferSize = pOutputBufferSize;
            this.Open();
        }

        internal abstract IEnumerable<RawRecord> CrossApply(RawRecord record);

        public override RawRecord Next()
        {
            // If the output buffer is not empty, returns a result.
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (InputOperator != null && !InputOperator.State())))
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }

            // Fills the input buffer by pulling from the input operator
            while (InputBuffer.Count() < InputBufferSize && InputOperator.State())
            {
                if (InputOperator != null && InputOperator.State())
                {
                    RawRecord result = InputOperator.Next();
                    if (result == null)
                    {
                        InputOperator.Close();
                    }
                    else
                    {
                        InputBuffer.Enqueue(result);
                    }
                }
            }

            var results = new List<RawRecord>();
            foreach (var record in InputBuffer)
                results.AddRange(CrossApply(record));

            foreach (var record in results)
                OutputBuffer.Enqueue(record);

            InputBuffer.Clear();
            if (OutputBuffer.Count <= 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }
    }

    internal class PropertiesOperator : TVFOperator
    {
        internal List<Tuple<string, int>> PropertyIdxList;

        internal PropertiesOperator(GraphViewExecutionOperator pInputOperatr, int pNewFieldIdx, List<Tuple<string, int>> pPropertyIdxList, int pInputBufferSize, int pOutputBufferSize)
            : base(pInputOperatr, pNewFieldIdx, pInputBufferSize, pOutputBufferSize)
        {
            PropertyIdxList = pPropertyIdxList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();
            foreach (var pair in PropertyIdxList)
            {
                var propName = pair.Item1;
                var propIdx = pair.Item2;
                var result = new RawRecord(record);
                record.fieldValues[NewFieldIdx] = propName + "->" + record.fieldValues[propIdx];
                results.Add(result);
            }

            return results;
        } 
    }

    internal class ValuesOperator : TVFOperator
    {
        internal List<int> ValuesIdxList;

        internal ValuesOperator(GraphViewExecutionOperator pInputOperator, int pNewFieldIdx, List<int> pValuesIdxList, int pInputBufferSize, int pOutputBufferSize)
            : base(pInputOperator, pNewFieldIdx, pInputBufferSize, pOutputBufferSize)
        {
            ValuesIdxList = pValuesIdxList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();
            foreach (var propIdx in ValuesIdxList)
            {
                var result = new RawRecord(record);
                record.fieldValues[NewFieldIdx] = record.fieldValues[propIdx];
                results.Add(result);
            }

            return results;
        }
    }

    internal class ConstantOperator : TVFOperator
    {
        internal string ConstantValue;

        internal ConstantOperator(GraphViewExecutionOperator pInputOperator, int pNewFieldIdx, string pConstantValue, int pInputBufferSize, int pOutputBufferSize)
            : base(pInputOperator, pNewFieldIdx, pInputBufferSize, pOutputBufferSize)
        {
            ConstantValue = pConstantValue;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var result = new RawRecord(record);
            record.fieldValues[NewFieldIdx] = ConstantValue;

            return new List<RawRecord> {result};
        }
    }

    internal class UnfoldOperator : TVFOperator
    {
        internal int FoldedFieldIdx;
        internal int FoldedMetaIdx;

        internal UnfoldOperator(GraphViewExecutionOperator pInputOperator, int pNewFieldIdx, int pFoldedFieldIdx, int pFoldedMetaIdx, int pInputBufferSize, int pOutputBufferSize)
            : base(pInputOperator, pNewFieldIdx, pInputBufferSize, pOutputBufferSize)
        {
            FoldedFieldIdx = pFoldedFieldIdx;
            FoldedMetaIdx = pFoldedMetaIdx;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();
            var metaInfo = record.fieldValues[FoldedMetaIdx].Split(',');
            var foldedList = record.fieldValues[FoldedFieldIdx];
            var start = 0;

            foreach (var offsetStr in metaInfo)
            {
                var offset = int.Parse(offsetStr);
                var result = new RawRecord(record);
                start += 1;
                record.fieldValues[NewFieldIdx] = foldedList.Substring(start, offset);
                results.Add(result);
                start += offset;
            }

            return results;
        }
    }
}
