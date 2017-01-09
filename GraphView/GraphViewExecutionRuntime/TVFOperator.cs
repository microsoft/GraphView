using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class TableValuedFunction : GraphViewExecutionOperator
    {
        internal GraphViewExecutionOperator InputOperator;
        protected Queue<RawRecord> OutputBuffer;
        protected int OutputBufferSize;

        internal TableValuedFunction(
            GraphViewExecutionOperator pInputOperator, 
            int pOutputBufferSize = 1000)
        {
            InputOperator = pInputOperator;
            OutputBufferSize = pOutputBufferSize;
            OutputBuffer = new Queue<RawRecord>(OutputBufferSize);
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

            while (OutputBuffer.Count < OutputBufferSize && InputOperator.State())
            {
                var srcRecord = InputOperator.Next();
                if (srcRecord == null)
                {
                    InputOperator.Close();
                    break;
                }

                var results = CrossApply(srcRecord);
                foreach (var rec in results)
                {
                    var resultRecord = new RawRecord(srcRecord);
                    resultRecord.Append(rec);
                    OutputBuffer.Enqueue(resultRecord);
                }
            }

            if (OutputBuffer.Count <= 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        public override void ResetState()
        {
            InputOperator.ResetState();
            OutputBuffer.Clear();
            this.Open();
        }
    }

    internal class PropertiesOperator : TableValuedFunction
    {
        internal List<Tuple<string, int>> PropertiesList;

        internal PropertiesOperator(GraphViewExecutionOperator pInputOperatr, List<Tuple<string, int>> pPropertiesList, int pOutputBufferSize = 1000)
            : base(pInputOperatr, pOutputBufferSize)
        {
            PropertiesList = pPropertiesList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();
            foreach (var pair in PropertiesList)
            {
                var propName = pair.Item1;
                var propIdx = pair.Item2;
                var result = new RawRecord(1);
                var fieldValue = record[propIdx];
                if (fieldValue == null) continue;;

                result.fieldValues[0] = propName + "->" + fieldValue;
                results.Add(result);
            }

            return results;
        } 
    }

    internal class ValuesOperator : TableValuedFunction
    {
        internal List<int> ValuesIdxList;

        internal ValuesOperator(GraphViewExecutionOperator pInputOperator, List<int> pValuesIdxList, int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            ValuesIdxList = pValuesIdxList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var results = new List<RawRecord>();
            foreach (var propIdx in ValuesIdxList)
            {
                var result = new RawRecord(1);
                var fieldValue = record[propIdx];
                if (fieldValue == null) continue;

                result.fieldValues[0] = fieldValue;
                results.Add(result);
            }

            return results;
        }
    }

    internal class ConstantOperator : TableValuedFunction
    {
        internal string ConstantValue;

        internal ConstantOperator(GraphViewExecutionOperator pInputOperator, string pConstantValue, int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
        {
            ConstantValue = pConstantValue;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var result = new RawRecord(1);
            result.fieldValues[0] = ConstantValue;

            return new List<RawRecord> {result};
        }
    }

    internal class UnfoldOperator : TableValuedFunction
    {
        internal int FoldedFieldIdx;
        internal int FoldedMetaIdx;

        internal UnfoldOperator(GraphViewExecutionOperator pInputOperator, int pFoldedFieldIdx, int pFoldedMetaIdx, int pOutputBufferSize = 1000)
            : base(pInputOperator, pOutputBufferSize)
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
                var result = new RawRecord(1);
                start += 1;
                result.fieldValues[0] = foldedList.Substring(start, offset);
                results.Add(result);
                start += offset;
            }

            return results;
        }
    }
}
