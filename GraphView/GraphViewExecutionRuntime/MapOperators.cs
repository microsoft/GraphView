using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    internal class MapOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the map function.
        private GraphViewExecutionOperator mapTraversal;


        private ContainerEnumerator sourceEnumerator;
        private List<RawRecord> inputBatch;
        private int batchSize;

        private HashSet<int> inputRecordSet;

        public MapOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator mapTraversal,
            ContainerEnumerator sourceEnumerator)
        {
            this.inputOp = inputOp;
            this.mapTraversal = mapTraversal;

            this.sourceEnumerator = sourceEnumerator;
            this.inputBatch = new List<RawRecord>();
            this.batchSize = KW_DEFAULT_BATCH_SIZE;

            this.inputRecordSet = new HashSet<int>();

            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.inputBatch.Any())
                {
                    RawRecord subTraversalRecord;
                    while (this.mapTraversal.State() && (subTraversalRecord = this.mapTraversal.Next()) != null)
                    {
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        if (this.inputRecordSet.Remove(subTraversalRecordIndex))
                        {
                            RawRecord resultRecord = inputBatch[subTraversalRecordIndex].GetRange(1);
                            resultRecord.Append(subTraversalRecord.GetRange(1));
                            return resultRecord;
                        }
                    }
                }

                this.inputBatch.Clear();
                this.inputRecordSet.Clear();
                RawRecord inputRecord;
                while (this.inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(this.inputBatch.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(inputRecord);

                    this.inputRecordSet.Add(this.inputBatch.Count);

                    inputBatch.Add(batchRawRecord);
                }

                if (!inputBatch.Any())
                {
                    this.Close();
                    return null;
                }

                this.sourceEnumerator.ResetTableCache(inputBatch);
                this.mapTraversal.ResetState();
            }

            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.mapTraversal.ResetState();
            this.inputBatch.Clear();
            this.inputRecordSet.Clear();
            this.Open();
        }
    }

    internal class FlatMapOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the flatMap function.
        private GraphViewExecutionOperator flatMapTraversal;
        private ContainerEnumerator sourceEnumerator;

        private List<RawRecord> inputBatch;

        private int batchSize;

        public FlatMapOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator flatMapTraversal,
            ContainerEnumerator sourceEnumerator,
            int batchSize = KW_DEFAULT_BATCH_SIZE)
        {
            this.inputOp = inputOp;
            this.flatMapTraversal = flatMapTraversal;
            this.sourceEnumerator = sourceEnumerator;
            this.batchSize = batchSize;

            this.inputBatch = new List<RawRecord>();

            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.inputBatch.Any())
                {
                    RawRecord subTraversalRecord;
                    while (flatMapTraversal.State() && (subTraversalRecord = flatMapTraversal.Next()) != null)
                    {
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        RawRecord resultRecord = inputBatch[subTraversalRecordIndex].GetRange(1);
                        resultRecord.Append(subTraversalRecord.GetRange(1));
                        return resultRecord;
                    }
                }

                this.inputBatch.Clear();
                RawRecord inputRecord;
                while (this.inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(this.inputBatch.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(inputRecord);

                    inputBatch.Add(batchRawRecord);
                }

                if (!inputBatch.Any())
                {
                    this.Close();
                    return null;
                }

                sourceEnumerator.ResetTableCache(inputBatch);
                flatMapTraversal.ResetState();
            }

            return null;
        }

        public override void ResetState()
        {
            this.sourceEnumerator.ResetState();
            this.inputBatch.Clear();
            this.inputOp.ResetState();
            this.flatMapTraversal.ResetState();
            this.Open();
        }
    }

    /// <summary>
    /// a little strange.
    /// Gremlin documentation says Local is branch type.
    /// In documentation and gremlin console, they both perform like follows:
    /// g.V().local(__.count()) : 1,1,1,1,1,1
    /// g.V().both().local(__.count()) : 3,1,3,3,1,1
    /// now the implementation is a map type.
    /// </summary>
    internal class LocalOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the local function.
        private GraphViewExecutionOperator localTraversal;
        private ContainerEnumerator sourceEnumerator;

        private List<RawRecord> inputBatch;

        private int batchSize;

        public LocalOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator localTraversal,
            ContainerEnumerator sourceEnumerator,
            int batchSize = KW_DEFAULT_BATCH_SIZE)
        {
            this.inputOp = inputOp;
            this.localTraversal = localTraversal;
            this.sourceEnumerator = sourceEnumerator;
            this.batchSize = batchSize;

            this.inputBatch = new List<RawRecord>();

            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.inputBatch.Any())
                {
                    RawRecord subTraversalRecord;
                    while (localTraversal.State() && (subTraversalRecord = localTraversal.Next()) != null)
                    {
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        RawRecord resultRecord = inputBatch[subTraversalRecordIndex].GetRange(1);
                        resultRecord.Append(subTraversalRecord.GetRange(1));
                        return resultRecord;
                    }
                }

                this.inputBatch.Clear();
                RawRecord inputRecord;
                while (this.inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(this.inputBatch.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(inputRecord);

                    inputBatch.Add(batchRawRecord);
                }

                if (!inputBatch.Any())
                {
                    this.Close();
                    return null;
                }

                this.sourceEnumerator.ResetTableCache(inputBatch);
                this.localTraversal.ResetState();
            }

            return null;
        }

        public override void ResetState()
        {
            this.sourceEnumerator.ResetState();
            this.inputBatch.Clear();
            this.inputOp.ResetState();
            this.localTraversal.ResetState();
            this.Open();
        }
    }

    internal class CoalesceOperator : GraphViewExecutionOperator
    {
        private List<GraphViewExecutionOperator> traversalList;
        private GraphViewExecutionOperator inputOp;

        // In batch mode, each RawRacord has an index,
        // so in this buffer dict, the keys are the indexes,
        // but in the Queue<RawRecord>, the indexes of RawRacords was already removed for output.
        private Dictionary<int, Queue<RawRecord>> traversalOutputBuffer;

        private ContainerEnumerator sourceEnumerator;
        private int batchSize;

        public CoalesceOperator(GraphViewExecutionOperator inputOp, ContainerEnumerator sourceEnumerator)
        {
            this.inputOp = inputOp;
            this.traversalList = new List<GraphViewExecutionOperator>();
            this.traversalOutputBuffer = new Dictionary<int, Queue<RawRecord>>();
            this.sourceEnumerator = sourceEnumerator;
            this.batchSize = KW_DEFAULT_BATCH_SIZE;
            this.Open();
        }

        public void AddTraversal(GraphViewExecutionOperator traversal)
        {
            traversalList.Add(traversal);
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.traversalOutputBuffer.Any())
                {
                    // Output in order
                    for (int i = 0; i < this.traversalOutputBuffer.Count; i++)
                    {
                        if (this.traversalOutputBuffer[i].Any())
                        {
                            return this.traversalOutputBuffer[i].Dequeue();
                        }
                    }

                    // nothing in any queue
                    this.traversalOutputBuffer.Clear();
                }

                List<RawRecord> inputBatch = new List<RawRecord>();
                // add to input batch.
                RawRecord inputRecord;

                // Indexes of Racords that will be transfered to next sub-traversal.
                // This set will be updated each time a sub-traversal finish.
                // TODO: RENAME IT
                HashSet<int> availableSrcSet = new HashSet<int>();
                while (inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(inputBatch.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(inputRecord);

                    availableSrcSet.Add(inputBatch.Count);
                    this.traversalOutputBuffer[inputBatch.Count] = new Queue<RawRecord>();

                    inputBatch.Add(batchRawRecord);
                }

                if (!inputBatch.Any())
                {
                    this.Close();
                    return null;
                }

                foreach (GraphViewExecutionOperator subTraversal in this.traversalList)
                {
                    HashSet<int> subOutputIndexSet = new HashSet<int>();
                    List<RawRecord> subTraversalSrc = availableSrcSet.Select(i => inputBatch[i]).ToList();

                    subTraversal.ResetState();
                    this.sourceEnumerator.ResetTableCache(subTraversalSrc);

                    RawRecord subTraversalRecord;
                    while (subTraversal.State() && (subTraversalRecord = subTraversal.Next()) != null)
                    {
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        RawRecord resultRecord = inputBatch[subTraversalRecordIndex].GetRange(1);
                        resultRecord.Append(subTraversalRecord.GetRange(1));
                        this.traversalOutputBuffer[subTraversalRecordIndex].Enqueue(resultRecord);
                        subOutputIndexSet.Add(subTraversalRecordIndex);
                    }

                    // Remove the racords that have output already.
                    availableSrcSet.ExceptWith(subOutputIndexSet);
                    if (!availableSrcSet.Any())
                    {
                        break;
                    }
                }

            }

            return null;

        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.traversalOutputBuffer?.Clear();
            this.Open();
        }
    }

    // sideEffect type && map type
    internal class SideEffectOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        private GraphViewExecutionOperator sideEffectTraversal;
        private ContainerEnumerator sourceEnumerator;

        private List<RawRecord> inputBatch;
        private Queue<RawRecord> outputBuffer;

        private int batchSize;

        public SideEffectOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator sideEffectTraversal,
            ContainerEnumerator sourceEnumerator,
            int batchSize = KW_DEFAULT_BATCH_SIZE)
        {
            this.inputOp = inputOp;
            this.sideEffectTraversal = sideEffectTraversal;
            this.sourceEnumerator = sourceEnumerator;

            this.inputBatch = new List<RawRecord>();
            this.batchSize = batchSize;
            this.outputBuffer = new Queue<RawRecord>();

            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.outputBuffer.Any())
                {
                    return outputBuffer.Dequeue();
                }

                this.inputBatch.Clear();
                RawRecord inputRecord;
                while (this.inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(this.inputBatch.Count.ToString(), JsonDataType.Int));
                    batchRawRecord.Append(inputRecord);

                    this.inputBatch.Add(batchRawRecord);
                    this.outputBuffer.Enqueue(inputRecord);
                }

                if (!inputBatch.Any())
                {
                    this.Close();
                    return null;
                }

                this.sourceEnumerator.ResetTableCache(this.inputBatch);
                this.sideEffectTraversal.ResetState();
                while (this.sideEffectTraversal.State())
                {
                    this.sideEffectTraversal.Next();
                }
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.sourceEnumerator.ResetState();
            this.inputBatch.Clear();
            this.inputOp.ResetState();
            this.sideEffectTraversal.ResetState();
            this.Open();
        }
    }
}
