using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [Serializable]
    internal class MapOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the map function.
        private GraphViewExecutionOperator mapTraversal;
        [NonSerialized]
        private Container container;

        [NonSerialized]
        private List<RawRecord> inputBatch;
        [NonSerialized]
        private int batchSize;
        [NonSerialized]
        private HashSet<int> inputRecordSet;

        private readonly bool isParallel;
        [NonSerialized]
        private bool hasFetchInput;

        public MapOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator mapTraversal,
            Container container,
            bool isParallel)
        {
            this.inputOp = inputOp;
            this.mapTraversal = mapTraversal;
            this.container = container;
            this.isParallel = isParallel;

            this.inputBatch = new List<RawRecord>();
            this.batchSize = KW_DEFAULT_BATCH_SIZE;

            this.inputRecordSet = new HashSet<int>();
            this.hasFetchInput = false;
            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.inputBatch.Any() || (this.isParallel && this.hasFetchInput))
                {
                    RawRecord subTraversalRecord;
                    while (this.mapTraversal.State() && (subTraversalRecord = this.mapTraversal.Next()) != null)
                    {
                        int startIndex = this.isParallel ? 2 : 1;
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        if (this.inputRecordSet.Remove(subTraversalRecordIndex))
                        {
                            RawRecord resultRecord = inputBatch[subTraversalRecordIndex].GetRange(startIndex);
                            resultRecord.Append(subTraversalRecord.GetRange(startIndex));
                            return resultRecord;
                        }
                    }

                    if (this.isParallel && !this.inputOp.State())
                    {
                        ReceiveOperator receiveOp = this.mapTraversal as ReceiveOperator;
                        if (!receiveOp.OtherContainerHasMoreResult())
                        {
                            this.Close();
                            return null;
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
                    if (this.isParallel)
                    {
                        batchRawRecord.Append(new StringField("-1", JsonDataType.Int));
                    }
                    batchRawRecord.Append(inputRecord);

                    this.inputRecordSet.Add(this.inputBatch.Count);

                    inputBatch.Add(batchRawRecord);
                }

                if (!this.inputBatch.Any() && !this.isParallel)
                {
                    this.Close();
                    return null;
                }

                this.hasFetchInput = true;
                this.container.HasMoreInput = this.inputOp.State();
                this.container.ResetTableCache(inputBatch);
                this.mapTraversal.ResetState();
            }

            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.mapTraversal.ResetState();
            this.container.Clear();

            this.inputBatch.Clear();
            this.inputRecordSet.Clear();
            this.hasFetchInput = false;
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.inputBatch = new List<RawRecord>();
            this.batchSize = KW_DEFAULT_BATCH_SIZE;
            this.inputRecordSet = new HashSet<int>();
            this.hasFetchInput = false;

            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.mapTraversal.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);
        }
    }

    [Serializable]
    internal class FlatMapOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the flatMap function.
        private GraphViewExecutionOperator flatMapTraversal;
        [NonSerialized]
        private Container container;

        [NonSerialized]
        private List<RawRecord> inputBatch;

        private int batchSize;

        private readonly bool isParallel;
        [NonSerialized]
        private bool hasFetchInput;

        public FlatMapOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator flatMapTraversal,
            Container container,
            bool isParallel,
            int batchSize = KW_DEFAULT_BATCH_SIZE)
        {
            this.inputOp = inputOp;
            this.flatMapTraversal = flatMapTraversal;
            this.container = container;
            this.isParallel = isParallel;
            this.batchSize = batchSize;

            this.inputBatch = new List<RawRecord>();
            this.hasFetchInput = false;
            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.inputBatch.Any() || (this.isParallel && this.hasFetchInput))
                {
                    RawRecord subTraversalRecord;
                    while (flatMapTraversal.State() && (subTraversalRecord = flatMapTraversal.Next()) != null)
                    {
                        int startIndex = this.isParallel ? 2 : 1;
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        RawRecord resultRecord = inputBatch[subTraversalRecordIndex].GetRange(startIndex);
                        resultRecord.Append(subTraversalRecord.GetRange(startIndex));
                        return resultRecord;
                    }

                    if (this.isParallel && !this.inputOp.State())
                    {
                        ReceiveOperator receiveOp = this.flatMapTraversal as ReceiveOperator;
                        if (!receiveOp.OtherContainerHasMoreResult())
                        {
                            this.Close();
                            return null;
                        }
                    }
                }

                this.inputBatch.Clear();
                RawRecord inputRecord;
                while (this.inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(this.inputBatch.Count.ToString(), JsonDataType.Int));
                    if (this.isParallel)
                    {
                        batchRawRecord.Append(new StringField("-1", JsonDataType.Int));
                    }
                    batchRawRecord.Append(inputRecord);

                    inputBatch.Add(batchRawRecord);
                }

                if (!this.inputBatch.Any() && !this.isParallel)
                {
                    this.Close();
                    return null;
                }

                this.hasFetchInput = true;
                this.container.HasMoreInput = this.inputOp.State();
                this.container.ResetTableCache(inputBatch);
                flatMapTraversal.ResetState();
            }

            return null;
        }

        public override void ResetState()
        {
            this.container.Clear();
            this.inputBatch.Clear();
            this.inputOp.ResetState();
            this.flatMapTraversal.ResetState();
            this.hasFetchInput = false;
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.inputBatch = new List<RawRecord>();
            this.hasFetchInput = false;

            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.flatMapTraversal.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);
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
    [Serializable]
    internal class LocalOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the local function.
        private GraphViewExecutionOperator localTraversal;
        [NonSerialized]
        private Container container;

        [NonSerialized]
        private List<RawRecord> inputBatch;

        private int batchSize;

        private readonly bool isParallel;
        [NonSerialized]
        private bool hasFetchInput;

        public LocalOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator localTraversal,
            Container container,
            bool isParallel,
            int batchSize = KW_DEFAULT_BATCH_SIZE)
        {
            this.inputOp = inputOp;
            this.localTraversal = localTraversal;
            this.container = container;
            this.isParallel = isParallel;
            this.batchSize = batchSize;

            this.inputBatch = new List<RawRecord>();
            this.hasFetchInput = false;
            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.inputBatch.Any() || (this.isParallel && this.hasFetchInput))
                {
                    RawRecord subTraversalRecord;
                    while (localTraversal.State() && (subTraversalRecord = localTraversal.Next()) != null)
                    {
                        int startIndex = this.isParallel ? 2 : 1;
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        RawRecord resultRecord = inputBatch[subTraversalRecordIndex].GetRange(startIndex);
                        resultRecord.Append(subTraversalRecord.GetRange(startIndex));
                        return resultRecord;
                    }

                    if (this.isParallel && !this.inputOp.State())
                    {
                        ReceiveOperator receiveOp = this.localTraversal as ReceiveOperator;
                        if (!receiveOp.OtherContainerHasMoreResult())
                        {
                            this.Close();
                            return null;
                        }
                    }
                }

                this.inputBatch.Clear();
                RawRecord inputRecord;
                while (this.inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(this.inputBatch.Count.ToString(), JsonDataType.Int));
                    if (this.isParallel)
                    {
                        batchRawRecord.Append(new StringField("-1", JsonDataType.Int));
                    }
                    batchRawRecord.Append(inputRecord);

                    this.inputBatch.Add(batchRawRecord);
                }

                if (!this.inputBatch.Any() && !this.isParallel)
                {
                    this.Close();
                    return null;
                }

                this.hasFetchInput = true;
                this.container.HasMoreInput = this.inputOp.State();
                this.container.ResetTableCache(inputBatch);
                this.localTraversal.ResetState();
            }

            return null;
        }

        public override void ResetState()
        {
            this.container.Clear();
            this.inputBatch.Clear();
            this.inputOp.ResetState();
            this.localTraversal.ResetState();
            this.hasFetchInput = false;
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.inputBatch = new List<RawRecord>();
            this.hasFetchInput = false;

            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.localTraversal.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);
        }
    }

    [Serializable]
    internal class CoalesceOperator : GraphViewExecutionOperator, ISerializable
    {
        private ContainerWithFlag container;
        private List<GraphViewExecutionOperator> traversalList;
        private GraphViewExecutionOperator inputOp;

        // In batch mode, each RawRacord has an index,
        // so in this buffer dict, the keys are the indexes,
        // but in the Queue<RawRecord>, the indexes of RawRacords was already removed for output.
        private SortedDictionary<int, Queue<RawRecord>> outputBuffer;

        private int batchSize;

        private readonly bool isParallel;

        public CoalesceOperator(GraphViewExecutionOperator inputOp, ContainerWithFlag container, bool isParallel = false)
        {
            this.inputOp = inputOp;
            this.container = container;
            this.traversalList = new List<GraphViewExecutionOperator>();
            this.outputBuffer = new SortedDictionary<int, Queue<RawRecord>>();
            this.isParallel = isParallel;
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
                if (this.outputBuffer.Any())
                {
                    // Output in order
                    foreach (Queue<RawRecord> queue in this.outputBuffer.Values)
                    {
                        if (queue.Any())
                        {
                            return queue.Dequeue();
                        }
                    }

                    // nothing in any queue
                    this.outputBuffer.Clear();
                }

                if (this.isParallel && !this.inputOp.State())
                {
                    ReceiveOperator receiveOp = this.traversalList.FirstOrDefault() as ReceiveOperator;
                    if (!receiveOp.OtherContainerHasMoreResult())
                    {
                        this.Close();
                        return null;
                    }
                }

                List<RawRecord> inputBatch = new List<RawRecord>();
                // add to input batch.
                RawRecord inputRecord;

                // Indexes of Racords that will be transfered to next sub-traversal.
                // This set will be updated each time a sub-traversal finish.
                while (inputBatch.Count < this.batchSize && this.inputOp.State() && (inputRecord = inputOp.Next()) != null)
                {
                    RawRecord batchRawRecord = new RawRecord();
                    batchRawRecord.Append(new StringField(inputBatch.Count.ToString(), JsonDataType.Int));
                    if (this.isParallel)
                    {
                        batchRawRecord.Append(new StringField("-1", JsonDataType.Int));
                    }
                    batchRawRecord.Append(inputRecord);

                    inputBatch.Add(batchRawRecord);
                }

                if (!this.isParallel && !inputBatch.Any())
                {
                    this.Close();
                    return null;
                }

                this.container.HasMoreInput = this.inputOp.State();
                this.container.ResetTableCache(inputBatch);
                int finishedCount = 0;
                foreach (GraphViewExecutionOperator subTraversal in this.traversalList)
                {
                    subTraversal.ResetState();

                    RawRecord subTraversalRecord;
                    List<int> deleteIndex = new List<int>();
                    while (subTraversal.State() && (subTraversalRecord = subTraversal.Next()) != null)
                    {
                        int startIndex = this.isParallel ? 2 : 1;
                        int subTraversalRecordIndex = int.Parse(subTraversalRecord[0].ToValue);
                        RawRecord resultRecord = this.container[subTraversalRecordIndex].GetRange(startIndex);
                        resultRecord.Append(subTraversalRecord.GetRange(startIndex));

                        if (!this.outputBuffer.ContainsKey(subTraversalRecordIndex))
                        {
                            this.outputBuffer[subTraversalRecordIndex] = new Queue<RawRecord>();
                            deleteIndex.Add(subTraversalRecordIndex);
                            finishedCount++;
                        }
                        this.outputBuffer[subTraversalRecordIndex].Enqueue(resultRecord);
                    }

                    this.container.Delete(deleteIndex);

                    if (!this.isParallel && finishedCount == this.container.Count)
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
            this.container.Clear();
            foreach (GraphViewExecutionOperator traversal in this.traversalList)
            {
                traversal.ResetState();
            }
            this.outputBuffer.Clear();
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.container = new ContainerWithFlag();
            foreach (GraphViewExecutionOperator traversal in this.traversalList)
            {
                EnumeratorOperator enumeratorOp = traversal.GetFirstOperator() as EnumeratorOperator;
                enumeratorOp.SetContainer(this.container);
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            GraphViewSerializer.SerializeList(info, "traversalList", this.traversalList);
            info.AddValue("inputOp", this.inputOp);
            info.AddValue("isParallel", this.isParallel);
        }

        protected CoalesceOperator(SerializationInfo info, StreamingContext context)
        {
            this.traversalList = GraphViewSerializer.DeserializeList<GraphViewExecutionOperator>(info, "traversalList");
            this.inputOp = (GraphViewExecutionOperator)info.GetValue("inputOp", typeof(GraphViewExecutionOperator));
            this.outputBuffer = new SortedDictionary<int, Queue<RawRecord>>();
            this.batchSize = KW_DEFAULT_BATCH_SIZE;
            this.isParallel = info.GetBoolean("isParallel");
        }
    }

    // sideEffect type && map type
    [Serializable]
    internal class SideEffectOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;
        private GraphViewExecutionOperator sideEffectTraversal;
        [NonSerialized]
        private Container container;

        [NonSerialized]
        private List<RawRecord> inputBatch;
        [NonSerialized]
        private Queue<RawRecord> outputBuffer;

        private int batchSize;

        public SideEffectOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator sideEffectTraversal,
            Container container,
            int batchSize = KW_DEFAULT_BATCH_SIZE)
        {
            this.inputOp = inputOp;
            this.sideEffectTraversal = sideEffectTraversal;
            this.container = container;

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

                this.container.ResetTableCache(this.inputBatch);
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
            this.container.Clear();
            this.inputBatch.Clear();
            this.inputOp.ResetState();
            this.sideEffectTraversal.ResetState();
            this.Open();
        }

        public override GraphViewExecutionOperator GetFirstOperator()
        {
            return this.inputOp.GetFirstOperator();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.inputBatch = new List<RawRecord>();
            this.outputBuffer = new Queue<RawRecord>();

            this.container = new Container();
            EnumeratorOperator enumeratorOp = this.sideEffectTraversal.GetFirstOperator() as EnumeratorOperator;
            enumeratorOp.SetContainer(this.container);
        }
    }
}
