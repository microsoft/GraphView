using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using GraphView.GraphViewDBPortal;
using Microsoft.Azure.Documents.Partitioning;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [DataContract]
    internal class FetchNodeOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly JsonQuery vertexQuery;

        private GraphViewCommand command;

        private IEnumerator<Tuple<VertexField, RawRecord>> verticesEnumerator;

        public FetchNodeOperator(GraphViewCommand command, JsonQuery vertexQuery)
        {
            this.Open();
            this.command = command;
            this.vertexQuery = vertexQuery;
            this.verticesEnumerator = command.Connection.CreateDatabasePortal().GetVerticesAndEdgesViaVertices(vertexQuery, this.command);
        }

        public override RawRecord Next()
        {
            if (this.verticesEnumerator.MoveNext()) {
                return this.verticesEnumerator.Current.Item2;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.verticesEnumerator = this.command.Connection.CreateDatabasePortal().GetVerticesAndEdgesViaVertices(this.vertexQuery, this.command);
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.command = SerializationData.Command;
            this.verticesEnumerator = this.command.Connection.CreateDatabasePortal().GetVerticesAndEdgesViaVertices(this.vertexQuery, this.command);
        }
    }

    [DataContract]
    internal class FetchEdgeOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private JsonQuery edgeQuery;
        private GraphViewCommand command;

        private IEnumerator<RawRecord> verticesAndEdgesEnumerator;

        public FetchEdgeOperator(GraphViewCommand command, JsonQuery edgeQuery)
        {
            this.Open();
            this.command = command;
            this.edgeQuery = edgeQuery;
            this.verticesAndEdgesEnumerator = command.Connection.CreateDatabasePortal().GetVerticesAndEdgesViaEdges(edgeQuery, this.command);
        }

        public override RawRecord Next()
        {
            if (this.verticesAndEdgesEnumerator.MoveNext())
            {
                return this.verticesAndEdgesEnumerator.Current;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.verticesAndEdgesEnumerator = this.command.Connection.CreateDatabasePortal().GetVerticesAndEdgesViaEdges(this.edgeQuery, this.command);
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.command = SerializationData.Command;
            this.verticesAndEdgesEnumerator = this.command.Connection.CreateDatabasePortal().GetVerticesAndEdgesViaEdges(this.edgeQuery, this.command);
        }
    }


    /// <summary>
    /// The operator that takes a list of records as source vertexes and 
    /// traverses to their one-hop or multi-hop neighbors. One-hop neighbors
    /// are defined in the adjacency lists of the sources. Multi-hop
    /// vertices are defined by a recursive function that has a sub-query
    /// specifying a single hop from a vertex to another and a boolean fuction 
    /// controlling when the recursion terminates (in other words, # of hops).  
    /// 
    /// This operators emulates the nested-loop join algorithm.
    /// </summary>
    [DataContract]
    internal class TraversalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private int outputBufferSize;
        [DataMember]
        private int batchSize = 5000;
        private Queue<RawRecord> outputBuffer;
        private GraphViewCommand command;
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        
        internal enum TraversalTypeEnum
        { Source, Sink, Other, Both }

        [DataMember]
        private int edgeFieldIndex;
        //
        // traversal type indicates which vertexId of the EdgeField would be used as the traversal destination 
        //
        [DataMember]
        private TraversalTypeEnum traversalType;

        // The query that describes predicates on the sink vertices and its properties to return.
        // It is null if the sink vertex has no predicates and no properties other than sink vertex ID
        // are to be returned.  
        [DataMember]
        private JsonQuery sinkVertexQuery;

        // Deprecated currently.
        // A list of index pairs, each specifying which field in the source record 
        // must match the field in the sink record. 
        // This list is not null when sink vertices have edges pointing back 
        // to the vertices other than the source vertices in the records by the input operator. 
        [DataMember]
        private List<Tuple<int, int>> matchingIndexes;

        public TraversalOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewCommand command,
            int edgeFieldIndex,
            TraversalTypeEnum traversalType,
            JsonQuery sinkVertexQuery,
            List<Tuple<int, int>> matchingIndexes,
            int outputBufferSize = 10000)
        {
            this.Open();
            this.inputOp = inputOp;
            this.command = command;
            this.edgeFieldIndex = edgeFieldIndex;
            this.traversalType = traversalType;
            this.sinkVertexQuery = sinkVertexQuery;
            this.matchingIndexes = matchingIndexes;
            this.outputBufferSize = outputBufferSize;
        }

        public override RawRecord Next()
        {
            if (this.outputBuffer == null) {
                this.outputBuffer = new Queue<RawRecord>(this.outputBufferSize);
            }

            while (this.outputBuffer.Count < this.outputBufferSize && this.inputOp.State())
            {
                //
                // <RawRecord, id, partition key>
                //
                List<Tuple<RawRecord, string, string>> inputSequence = new List<Tuple<RawRecord, string, string>>(this.batchSize);

                // Loads a batch of source records
                for (int i = 0; i < this.batchSize && this.inputOp.State(); i++)
                {
                    RawRecord record = this.inputOp.Next();
                    if (record == null) {
                        break;
                    }

                    EdgeField edgeField = record[this.edgeFieldIndex] as EdgeField;
                    Debug.Assert(edgeField != null, "edgeField != null");

                    switch (this.traversalType)
                    {
                        case TraversalTypeEnum.Source:
                            inputSequence.Add(new Tuple<RawRecord, string, string>(record, edgeField.OutV, edgeField.OutVPartition));
                            break;
                        case TraversalTypeEnum.Sink:
                            inputSequence.Add(new Tuple<RawRecord, string, string>(record, edgeField.InV, edgeField.InVPartition));
                            break;
                        case TraversalTypeEnum.Other:
                            inputSequence.Add(new Tuple<RawRecord, string, string>(record, edgeField.OtherV, edgeField.OtherVPartition));
                            break;
                        case TraversalTypeEnum.Both:
                            inputSequence.Add(new Tuple<RawRecord, string, string>(record, edgeField.InV, edgeField.InVPartition));
                            inputSequence.Add(new Tuple<RawRecord, string, string>(record, edgeField.OutV, edgeField.OutVPartition));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                // When sinkVertexQuery is null, only sink vertices' IDs are to be returned. 
                // As a result, there is no need to send queries the underlying system to retrieve 
                // the sink vertices.  
                if (sinkVertexQuery == null)
                {
                    foreach (Tuple<RawRecord, string, string> pair in inputSequence)
                    {
                        RawRecord resultRecord = new RawRecord { fieldValues = new List<FieldObject>() };
                        resultRecord.Append(pair.Item1);
                        resultRecord.Append(new ValuePropertyField(DocumentDBKeywords.KW_DOC_ID, pair.Item2,
                            JsonDataType.String, (VertexField) null));
                        outputBuffer.Enqueue(resultRecord);
                    }

                    continue;
                }

                // Groups records returned by sinkVertexQuery by sink vertices' references
                Dictionary<string, List<RawRecord>> sinkVertexCollection = new Dictionary<string, List<RawRecord>>(GraphViewConnection.InClauseLimit);

                HashSet<string> sinkReferenceSet = new HashSet<string>();
                HashSet<string> sinkPartitionSet = new HashSet<string>();

                // Given a list of sink references, sends queries to the underlying system
                // to retrieve the sink vertices. To reduce the number of queries to send,
                // we pack multiple sink references in one query using the IN clause, i.e., 
                // IN (ref1, ref2, ...). Since the total number of references to locate may exceed
                // the limit that is allowed in the IN clause, we may need to send more than one 
                // query to retrieve all sink vertices. 
                int j = 0;
                while (j < inputSequence.Count)
                {
                    sinkReferenceSet.Clear();
                    sinkPartitionSet.Clear();

                    //TODO: Verify whether DocumentDB still has inClauseLimit
                    while (sinkReferenceSet.Count < GraphViewConnection.InClauseLimit && j < inputSequence.Count)
                    {
                        string sinkReferenceId = inputSequence[j].Item2;
                        string sinkPartitionKey = inputSequence[j].Item3;
                        sinkReferenceSet.Add(sinkReferenceId);
                        if (!string.IsNullOrEmpty(sinkPartitionKey))
                            sinkPartitionSet.Add(sinkPartitionKey);
                        j++;
                    }

                    var toSendQuery = new JsonQuery(this.sinkVertexQuery);
                    toSendQuery.WhereConjunction(new WInPredicate(new WColumnReferenceExpression(toSendQuery.NodeAlias, KW_DOC_ID), sinkReferenceSet.ToList()),
                        BooleanBinaryExpressionType.And);

                    using (DbPortal databasePortal = this.command.Connection.CreateDatabasePortal())
                    {
                        IEnumerator<Tuple<VertexField, RawRecord>> verticesEnumerator = databasePortal.GetVerticesAndEdgesViaVertices(toSendQuery, this.command);

                        // The following lines are added for debugging convenience
                        // It nearly does no harm to performance
                        List<Tuple<VertexField, RawRecord>> temp = new List<Tuple<VertexField, RawRecord>>();
                        try
                        {
                            while (verticesEnumerator.MoveNext())
                            {
                                temp.Add(verticesEnumerator.Current);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(toSendQuery.ToString(DatabaseType.DocumentDB));
                            throw e;
                        }                        

                        foreach (Tuple<VertexField, RawRecord> tuple in temp)
                        {
                            VertexField vfield = tuple.Item1;
                            if (!sinkVertexCollection.ContainsKey(vfield.VertexId)) {
                                sinkVertexCollection.Add(vfield.VertexId, new List<RawRecord>());
                            }
                            sinkVertexCollection[vfield.VertexId].Add(tuple.Item2);
                        }
                    }
                }

                foreach (Tuple<RawRecord, string, string> pair in inputSequence)
                {
                    if (!sinkVertexCollection.ContainsKey(pair.Item2)) {
                        continue;
                    }

                    RawRecord sourceRec = pair.Item1;
                    List<RawRecord> sinkRecList = sinkVertexCollection[pair.Item2];
                    
                    foreach (RawRecord sinkRec in sinkRecList)
                    {
                        if (this.matchingIndexes != null && this.matchingIndexes.Count > 0)
                        {
                            int k = 0;
                            for (; k < this.matchingIndexes.Count; k++)
                            {
                                int sourceMatchIndex = this.matchingIndexes[k].Item1;
                                int sinkMatchIndex = this.matchingIndexes[k].Item2;
                                if (!sourceRec[sourceMatchIndex].ToValue.Equals(sinkRec[sinkMatchIndex].ToValue, StringComparison.OrdinalIgnoreCase))
                                {
                                    break;
                                }
                            }

                            // The source-sink record pair is the result only when it passes all matching tests. 
                            if (k < this.matchingIndexes.Count)
                            {
                                continue;
                            }
                        }

                        RawRecord resultRec = new RawRecord(sourceRec);
                        resultRec.Append(sinkRec);

                        this.outputBuffer.Enqueue(resultRec);
                    }
                }
            }

            if (this.outputBuffer.Count == 0) {
                this.Close();
                return null;
            }
            else {
                return this.outputBuffer.Dequeue();
            }
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            outputBuffer?.Clear();
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.command = SerializationData.Command;
        }
    }

    [DataContract]
    internal class FilterOperator : GraphViewExecutionOperator
    {
        [DataMember]
        public GraphViewExecutionOperator Input { get; private set; }
        [DataMember]
        public BooleanFunction Func { get; private set; }

        public FilterOperator(GraphViewExecutionOperator input, BooleanFunction func)
        {
            Input = input;
            Func = func;
            Open();
        }

        public override RawRecord Next()
        {
            RawRecord rec;

            while (Input.State() && (rec = Input.Next()) != null)
            {
                if (Func.Evaluate(rec))
                {
                    return rec;
                }
            }


            this.Close();
            return null;
        }

        public override void ResetState()
        {
            Input.ResetState();
            Open();
        }
    }

    [DataContract]
    internal class FilterInBatchOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly GraphViewExecutionOperator input;
        [DataMember]
        private readonly BooleanFunction func;
        [DataMember]
        private readonly int batchSize;

        private int index;
        private List<RawRecord> inputBatch;
        private HashSet<int> returnIndexes;

        public FilterInBatchOperator(GraphViewExecutionOperator input, BooleanFunction func, int batchSize = 1000)
        {
            this.input = input;
            this.func = func;
            this.batchSize = batchSize;
            this.index = 0;
            this.inputBatch = new List<RawRecord>();
            this.returnIndexes = new HashSet<int>();

            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                while (this.index < this.inputBatch.Count)
                {
                    RawRecord inputRecord = this.inputBatch[this.index];
                    int inputIndex = this.index++;
                    if (this.returnIndexes.Contains(inputIndex))
                    {
                        return inputRecord.GetRange(1, inputRecord.Length - 1);
                    }
                }

                if (!this.input.State())
                {
                    this.Close();
                    return null;
                }

                this.index = 0;
                this.inputBatch.Clear();
                RawRecord rec;
                while (this.inputBatch.Count < this.batchSize && this.input.State() && (rec = this.input.Next()) != null)
                {
                    RawRecord inputRecord = new RawRecord();
                    inputRecord.Append(new StringField(this.inputBatch.Count.ToString(), JsonDataType.Int));
                    inputRecord.Append(rec);
                    this.inputBatch.Add(inputRecord);
                }

                this.returnIndexes = this.func.EvaluateInBatch(this.inputBatch);
            }

            return null;
        }

        public override void ResetState()
        {
            this.input.ResetState();
            this.inputBatch.Clear();
            this.returnIndexes.Clear();
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.index = 0;
            this.inputBatch = new List<RawRecord>();
            this.returnIndexes = new HashSet<int>();
        }
    }

    [DataContract]
    internal class CartesianProductOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator leftInput;
        [DataMember]
        private GraphViewExecutionOperator rightInput;

        private EnumeratorOperator rightEnumerator;
        private Container container;
        private RawRecord leftRecord;

        private bool needInitialize;

        public CartesianProductOperator(
            GraphViewExecutionOperator leftInput, 
            GraphViewExecutionOperator rightInput)
        {
            this.leftInput = leftInput;
            this.rightInput = rightInput;

            this.container = new Container();
            int containerIndex = SerializationData.AddContainers(this.container);
            this.rightEnumerator = new EnumeratorOperator(this.container, containerIndex);

            this.needInitialize = true;
            leftRecord = null;
            Open();
        }

        public override RawRecord Next()
        {
            if (this.needInitialize)
            {
                List<RawRecord> inputBuffer = new List<RawRecord>();
                RawRecord inputRecord;
                while (this.rightInput.State() && (inputRecord = this.rightInput.Next()) != null)
                {
                    inputBuffer.Add(inputRecord);
                }

                this.container.ResetTableCache(inputBuffer);
                this.needInitialize = false;
            }

            RawRecord cartesianRecord = null;

            while (cartesianRecord == null && State())
            {
                if (leftRecord == null && leftInput.State())
                {
                    leftRecord = leftInput.Next();
                }

                if (leftRecord == null)
                {
                    Close();
                    break;
                }
                else
                {
                    if (rightEnumerator.MoveNext())
                    {
                        RawRecord rightRecord = rightEnumerator.Current;
                        cartesianRecord = new RawRecord(leftRecord);
                        cartesianRecord.Append(rightRecord);
                    }
                    else
                    {
                        // For the current left record, the enumerator on the right input has reached the end.
                        // Moves to the next left record and resets the enumerator.
                        rightEnumerator.ResetState();
                        leftRecord = null;
                    }
                }
            }

            return cartesianRecord;
        }

        public override void ResetState()
        {
            this.leftInput.ResetState();
            this.rightInput.ResetState();
            this.container.Clear();
            this.rightEnumerator.ResetState();
            this.needInitialize = true;
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.container = new Container();
            int containerIndex = SerializationData.AddContainers(this.container);
            this.rightEnumerator = new EnumeratorOperator(this.container, containerIndex);

            this.needInitialize = true;
            leftRecord = null;
        }
    }

    [DataContract]
    [KnownType(typeof(OrderInBatchOperator))]
    internal class OrderOperator : GraphViewExecutionOperator
    {
        [DataMember]
        protected GraphViewExecutionOperator inputOp;
        protected List<RawRecord> inputBuffer;
        protected int returnIndex;

        [DataMember]
        protected List<Tuple<ScalarFunction, IComparer>> orderByElements;

        public OrderOperator(GraphViewExecutionOperator inputOp, List<Tuple<ScalarFunction, IComparer>> orderByElements)
        {
            this.Open();
            this.inputOp = inputOp;
            this.orderByElements = orderByElements;
            this.returnIndex = 0;
        }

        public override RawRecord Next()
        {
            if (this.inputBuffer == null)
            {
                this.inputBuffer = new List<RawRecord>();

                RawRecord inputRec = null;
                while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null) {
                    this.inputBuffer.Add(inputRec);
                }

                this.inputBuffer.Sort((x, y) =>
                {
                    int ret = 0;
                    foreach (Tuple<ScalarFunction, IComparer> orderByElement in this.orderByElements)
                    {
                        ScalarFunction byFunction = orderByElement.Item1;

                        FieldObject xKey = byFunction.Evaluate(x);
                        if (xKey == null) {
                            throw new GraphViewException("The provided traversal or property name of Order does not map to a value.");
                        }

                        FieldObject yKey = byFunction.Evaluate(y);
                        if (yKey == null) {
                            throw new GraphViewException("The provided traversal or property name of Order does not map to a value.");
                        }

                        IComparer comparer = orderByElement.Item2;
                        ret = comparer.Compare(xKey.ToObject(), yKey.ToObject());

                        if (ret != 0) break;
                    }
                    return ret;
                });
            }

            while (this.returnIndex < this.inputBuffer.Count) {
                return this.inputBuffer[this.returnIndex++];
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputBuffer = null;
            this.inputOp.ResetState();
            this.returnIndex = 0;

            this.Open();
        }
    }

    [DataContract]
    internal class OrderInBatchOperator : OrderOperator
    {
        private RawRecord firstRecordInGroup;

        internal OrderInBatchOperator(
            GraphViewExecutionOperator inputOp,
            List<Tuple<ScalarFunction, IComparer>> orderByElements)
            : base(inputOp, orderByElements)
        {
            this.inputBuffer = new List<RawRecord>();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                while (this.returnIndex < this.inputBuffer.Count)
                {
                    return this.inputBuffer[this.returnIndex++];
                }
                this.returnIndex = 0;
                this.inputBuffer.Clear();

                if (this.firstRecordInGroup == null && this.inputOp.State())
                {
                    this.firstRecordInGroup = this.inputOp.Next();
                }

                if (this.firstRecordInGroup == null)
                {
                    this.Close();
                    return null;
                }

                this.inputBuffer.Add(this.firstRecordInGroup);
                // Collect one group into the buffer
                RawRecord rec = null;
                while (this.inputOp.State() &&
                       (rec = this.inputOp.Next()) != null &&
                       rec[0].ToValue == this.firstRecordInGroup[0].ToValue)
                {
                    this.inputBuffer.Add(rec);
                }

                this.firstRecordInGroup = rec;

                this.inputBuffer.Sort((x, y) =>
                {
                    int ret = 0;
                    foreach (Tuple<ScalarFunction, IComparer> orderByElement in this.orderByElements)
                    {
                        ScalarFunction byFunction = orderByElement.Item1;

                        FieldObject xKey = byFunction.Evaluate(x);
                        if (xKey == null)
                        {
                            throw new GraphViewException("The provided traversal or property name of Order does not map to a value.");
                        }

                        FieldObject yKey = byFunction.Evaluate(y);
                        if (yKey == null)
                        {
                            throw new GraphViewException("The provided traversal or property name of Order does not map to a value.");
                        }

                        IComparer comparer = orderByElement.Item2;
                        ret = comparer.Compare(xKey.ToObject(), yKey.ToObject());

                        if (ret != 0) break;
                    }
                    return ret;
                });
            }

            return null;
        }

        public override void ResetState()
        {
            base.ResetState();
            this.inputBuffer = new List<RawRecord>();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.inputBuffer = new List<RawRecord>();
        }
    }

    [DataContract]
    internal class OrderLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int inputObjectIndex;
        [DataMember]
        private List<Tuple<ScalarFunction, IComparer>> orderByElements;

        [DataMember]
        private List<string> populateColumns;

        public OrderLocalOperator(
            GraphViewExecutionOperator inputOp, 
            int inputObjectIndex, 
            List<Tuple<ScalarFunction, IComparer>> orderByElements,
            List<string> populateColumns)
        {
            this.inputOp = inputOp;
            this.inputObjectIndex = inputObjectIndex;
            this.orderByElements = orderByElements;
            this.Open();

            this.populateColumns = populateColumns;
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (this.inputOp.State() && (srcRecord = this.inputOp.Next()) != null)
            {
                RawRecord newRecord = new RawRecord(srcRecord);

                FieldObject inputObject = srcRecord[this.inputObjectIndex];
                FieldObject orderedObject;
                if (inputObject is CollectionField)
                {
                    CollectionField inputCollection = (CollectionField)inputObject;
                    CollectionField orderedCollection = new CollectionField(inputCollection);
                    orderedCollection.Collection.Sort((x, y) =>
                    {
                        int ret = 0;
                        foreach (Tuple<ScalarFunction, IComparer> tuple in this.orderByElements)
                        {
                            ScalarFunction byFunction = tuple.Item1;

                            RawRecord initCompose1RecordOfX = new RawRecord();
                            initCompose1RecordOfX.Append(x);
                            FieldObject xKey = byFunction.Evaluate(initCompose1RecordOfX);
                            if (xKey == null) {
                                throw new GraphViewException("The provided traversal or property name of Order(local) does not map to a value.");
                            }

                            RawRecord initCompose1RecordOfY = new RawRecord();
                            initCompose1RecordOfY.Append(y);
                            FieldObject yKey = byFunction.Evaluate(initCompose1RecordOfY);
                            if (yKey == null) {
                                throw new GraphViewException("The provided traversal or property name of Order(local) does not map to a value.");
                            }

                            IComparer comparer = tuple.Item2;
                            ret = comparer.Compare(xKey.ToObject(), yKey.ToObject());

                            if (ret != 0) break;
                        }
                        return ret;
                    });
                    orderedObject = orderedCollection;
                }
                else if (inputObject is MapField)
                {
                    MapField inputMap = (MapField) inputObject;
                    List<EntryField> entries = inputMap.ToList();

                    entries.Sort((x, y) =>
                    {
                        int ret = 0;
                        foreach (Tuple<ScalarFunction, IComparer> tuple in this.orderByElements)
                        {
                            ScalarFunction byFunction = tuple.Item1;

                            RawRecord initKeyValuePairRecordOfX = new RawRecord();
                            initKeyValuePairRecordOfX.Append(x);
                            FieldObject xKey = byFunction.Evaluate(initKeyValuePairRecordOfX);
                            if (xKey == null) {
                                throw new GraphViewException("The provided traversal or property name of Order(local) does not map to a value.");
                            }
                            
                            RawRecord initKeyValuePairRecordOfY = new RawRecord();
                            initKeyValuePairRecordOfY.Append(y);
                            FieldObject yKey = byFunction.Evaluate(initKeyValuePairRecordOfY);
                            if (yKey == null) {
                                throw new GraphViewException("The provided traversal or property name of Order(local) does not map to a value.");
                            }
                            
                            IComparer comparer = tuple.Item2;
                            ret = comparer.Compare(xKey.ToObject(), yKey.ToObject());

                            if (ret != 0) break;
                        }
                        return ret;
                    });

                    MapField orderedMapField = new MapField();
                    foreach (EntryField entry in entries) {
                        orderedMapField.Add(entry.Key, entry.Value);
                    }
                    orderedObject = orderedMapField;
                }
                else {
                    orderedObject = inputObject;
                }

                RawRecord flatRawRecord = orderedObject.FlatToRawRecord(this.populateColumns);
                newRecord.Append(flatRawRecord);
                return newRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class ProjectOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private List<ScalarFunction> selectScalarList;
        [DataMember]
        private GraphViewExecutionOperator inputOp;

        private RawRecord currentRecord;

        public ProjectOperator(GraphViewExecutionOperator inputOp)
        {
            this.Open();
            this.inputOp = inputOp;
            selectScalarList = new List<ScalarFunction>();
        }

        public void AddSelectScalarElement(ScalarFunction scalarFunction)
        {
            selectScalarList.Add(scalarFunction);
        }

        public override RawRecord Next()
        {
            currentRecord = inputOp.State() ? inputOp.Next() : null;
            if (currentRecord == null)
            {
                Close();
                return null;
            }

            RawRecord selectRecord = new RawRecord(selectScalarList.Count);
            int index = 0;
            foreach (var scalarFunction in selectScalarList)
            {
                // TODO: Skip * for now, need refactor
                if (scalarFunction != null)
                {
                    FieldObject result = scalarFunction.Evaluate(currentRecord);
                    selectRecord.fieldValues[index++] = result;
                }
                else
                {
                    selectRecord.fieldValues[index++] = null;
                }
            }

            return selectRecord;
        }

        public override void ResetState()
        {
            currentRecord = null;
            inputOp.ResetState();
            Open();
        }
    }

    [DataContract]
    [KnownType(typeof(ProjectAggregationInBatch))]
    internal class ProjectAggregation : GraphViewExecutionOperator
    {
        [DataMember]
        protected List<Tuple<IAggregateFunction, List<ScalarFunction>>> aggregationSpecs;
        [DataMember]
        protected GraphViewExecutionOperator inputOp;

        public ProjectAggregation(GraphViewExecutionOperator inputOp)
        {
            this.inputOp = inputOp;
            aggregationSpecs = new List<Tuple<IAggregateFunction, List<ScalarFunction>>>();
            Open();
        }

        public void AddAggregateSpec(IAggregateFunction aggrFunc, List<ScalarFunction> aggrInput)
        {
            aggregationSpecs.Add(new Tuple<IAggregateFunction, List<ScalarFunction>>(aggrFunc, aggrInput));
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            foreach (var aggr in aggregationSpecs)
            {
                if (aggr.Item1 != null)
                {
                    aggr.Item1.Init();
                }
            }
            Open();
        }

        public override RawRecord Next()
        {
            if (!State())
                return null;

            foreach (var aggr in aggregationSpecs)
            {
                if (aggr.Item1 != null)
                {
                    aggr.Item1.Init();
                }
            }

            RawRecord inputRec = null;
            while (inputOp.State() && (inputRec = inputOp.Next()) != null)
            {
                foreach (var aggr in aggregationSpecs)
                {
                    IAggregateFunction aggregate = aggr.Item1;
                    List<ScalarFunction> parameterFunctions = aggr.Item2;

                    if (aggregate == null)
                    {
                        continue;
                    }

                    FieldObject[] paraList = new FieldObject[aggr.Item2.Count];
                    for(int i = 0; i < parameterFunctions.Count; i++)
                    {
                        paraList[i] = parameterFunctions[i].Evaluate(inputRec); 
                    }

                    aggregate.Accumulate(paraList);
                }
            }

            RawRecord outputRec = new RawRecord();
            foreach (var aggr in aggregationSpecs)
            {
                if (aggr.Item1 != null)
                {
                    outputRec.Append(aggr.Item1.Terminate());
                }
                else
                {
                    outputRec.Append((StringField)null);
                }
            }

            Close();
            return outputRec;
        }
    }

    [DataContract]
    internal class ProjectAggregationInBatch : ProjectAggregation
    {
        private RawRecord firstRecordInGroup = null;

        internal ProjectAggregationInBatch(GraphViewExecutionOperator inputOp) : base(inputOp)
        { }

        public RawRecord GetNoAccumulateRecord(int index)
        {
            foreach (Tuple<IAggregateFunction, List<ScalarFunction>> aggr in this.aggregationSpecs)
            {
                if (aggr.Item1 == null)
                {
                    continue;
                }
                aggr.Item1.Init();
            }

            RawRecord noAccumulateRecord = new RawRecord();
            noAccumulateRecord.Append(new StringField(index.ToString(), JsonDataType.Int));
            foreach (Tuple<IAggregateFunction, List<ScalarFunction>> aggr in this.aggregationSpecs)
            {
                if (aggr.Item1 != null)
                {
                    noAccumulateRecord.Append(aggr.Item1.Terminate());
                }
                else
                {
                    noAccumulateRecord.Append((FieldObject)null);
                }
            }

            return noAccumulateRecord;
        }

        public override RawRecord Next()
        {
            if (this.firstRecordInGroup == null && this.State())
            {
                this.firstRecordInGroup = this.inputOp.Next();
            }

            if (this.firstRecordInGroup == null)
            {
                this.Close();
                return null;
            }

            foreach (Tuple<IAggregateFunction, List<ScalarFunction>> aggr in this.aggregationSpecs)
            {
                if (aggr.Item1 == null)
                {
                    continue;
                }

                aggr.Item1.Init();
                FieldObject[] paraList = new FieldObject[aggr.Item2.Count];
                for (int index = 0; index < aggr.Item2.Count; index++)
                {
                    paraList[index] = aggr.Item2[index].Evaluate(this.firstRecordInGroup);
                }

                aggr.Item1.Accumulate(paraList);
            }

            RawRecord rec = this.inputOp.Next();
            while (rec != null && rec[0].ToValue == this.firstRecordInGroup[0].ToValue)
            {
                foreach (Tuple<IAggregateFunction, List<ScalarFunction>> aggr in this.aggregationSpecs)
                {
                    IAggregateFunction aggregate = aggr.Item1;

                    if (aggregate == null)
                    {
                        continue;
                    }

                    FieldObject[] paraList = new FieldObject[aggr.Item2.Count];
                    for (int index = 0; index < aggr.Item2.Count; index++)
                    {
                        paraList[index] = aggr.Item2[index].Evaluate(rec);
                    }

                    aggr.Item1.Accumulate(paraList);
                }

                rec = this.inputOp.Next();
            }

            RawRecord outputRec = new RawRecord();
            outputRec.Append(this.firstRecordInGroup[0]);
            foreach (Tuple<IAggregateFunction, List<ScalarFunction>> aggr in this.aggregationSpecs)
            {
                if (aggr.Item1 != null)
                {
                    outputRec.Append(aggr.Item1.Terminate());
                }
                else
                {
                    outputRec.Append((FieldObject)null);
                }
            }

            this.firstRecordInGroup = rec;

            return outputRec;
        }
    }
    
    [DataContract]
    [KnownType(typeof(DeduplicateInBatchOperator))]
    internal class DeduplicateOperator : GraphViewExecutionOperator
    {
        [DataMember]
        protected GraphViewExecutionOperator inputOp;
        protected HashSet<CollectionField> compositeDedupKeySet;
        [DataMember]
        protected List<ScalarFunction> compositeDedupKeyFuncList;

        internal DeduplicateOperator(GraphViewExecutionOperator inputOperator, List<ScalarFunction> compositeDedupKeyFuncList)
        {
            this.inputOp = inputOperator;
            this.compositeDedupKeyFuncList = compositeDedupKeyFuncList;
            this.compositeDedupKeySet = new HashSet<CollectionField>();

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;
                
            while (this.inputOp.State() && (srcRecord = this.inputOp.Next()) != null)
            {
                List<FieldObject> keys = new List<FieldObject>();
                for (int dedupKeyIndex = 0; dedupKeyIndex < compositeDedupKeyFuncList.Count; dedupKeyIndex++)
                {
                    ScalarFunction getDedupKeyFunc = compositeDedupKeyFuncList[dedupKeyIndex];
                    FieldObject key = getDedupKeyFunc.Evaluate(srcRecord);
                    if (key == null) {
                        throw new GraphViewException("The provided traversal or property name of Dedup does not map to a value.");
                    }

                    keys.Add(key);
                }

                CollectionField compositeDedupKey = new CollectionField(keys);
                if (!this.compositeDedupKeySet.Contains(compositeDedupKey))
                {
                    this.compositeDedupKeySet.Add(compositeDedupKey);
                    return srcRecord;
                }
            }

            this.Close();
            this.compositeDedupKeySet.Clear();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.compositeDedupKeySet.Clear();

            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.compositeDedupKeySet = new HashSet<CollectionField>();
        }
    }

    [DataContract]
    internal class DeduplicateInBatchOperator : DeduplicateOperator
    {
        private RawRecord firstRecordInGroup;
        [DataMember]
        private bool newGroup;

        internal DeduplicateInBatchOperator(
            GraphViewExecutionOperator inputOperator,
            List<ScalarFunction> compositeDedupKeyFuncList)
            : base(inputOperator, compositeDedupKeyFuncList)
        {
            this.newGroup = false;
        }

        private bool IsUniqueRecord(RawRecord record)
        {
            List<FieldObject> keys = new List<FieldObject>();
            foreach (ScalarFunction getDedupKeyFunc in this.compositeDedupKeyFuncList)
            {
                FieldObject key = getDedupKeyFunc.Evaluate(record);
                if (key == null)
                {
                    throw new GraphViewException("The provided traversal or property name of Dedup does not map to a value.");
                }

                keys.Add(key);
            }

            CollectionField compositeDedupKey = new CollectionField(keys);

            if (!this.compositeDedupKeySet.Contains(compositeDedupKey))
            {
                this.compositeDedupKeySet.Add(compositeDedupKey);
                return true;
            }

            return false;
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                RawRecord rec = null;
                while (this.newGroup &&
                    this.inputOp.State() &&
                    (rec = this.inputOp.Next()) != null &&
                    rec[0].ToValue == this.firstRecordInGroup[0].ToValue)
                {
                    if (this.IsUniqueRecord(rec))
                    {
                        return rec;
                    }
                }

                this.newGroup = false;
                this.firstRecordInGroup = rec;
                this.compositeDedupKeySet.Clear();

                if (this.firstRecordInGroup == null && this.inputOp.State())
                {
                    this.firstRecordInGroup = this.inputOp.Next();
                }

                if (this.firstRecordInGroup == null)
                {
                    this.Close();
                    return null;
                }

                this.newGroup = true;
                if (this.IsUniqueRecord(this.firstRecordInGroup))
                {
                    return this.firstRecordInGroup;
                }
            }

            return null;
        }

        public override void ResetState()
        {
            base.ResetState();
            this.newGroup = false;
        }
    }

    [DataContract]
    internal class DeduplicateLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private ScalarFunction getInputObjectionFunc;
        
        internal DeduplicateLocalOperator(
            GraphViewExecutionOperator inputOperator, 
            ScalarFunction getInputObjectionFunc)
        {
            this.inputOp = inputOperator;
            this.getInputObjectionFunc = getInputObjectionFunc;

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                RawRecord result = new RawRecord(currentRecord);
                FieldObject inputObject = this.getInputObjectionFunc.Evaluate(currentRecord);

                HashSet<Object> localObjectsSet = new HashSet<Object>();
                CollectionField uniqueCollection = new CollectionField();

                if (inputObject is CollectionField)
                {
                    CollectionField inputCollection = (CollectionField)inputObject;
                    
                    foreach (FieldObject localFieldObject in inputCollection.Collection)
                    {
                        Object localObj = localFieldObject.ToObject();
                        if (!localObjectsSet.Contains(localObj))
                        {
                            uniqueCollection.Collection.Add(localFieldObject);
                            localObjectsSet.Add(localObj);
                        }
                    }
                }
                else if (inputObject is PathField)
                {
                    PathField inputPath = (PathField)inputObject;

                    foreach (PathStepField pathStep in inputPath.Path.Cast<PathStepField>())
                    {
                        Object localObj = pathStep.ToObject();
                        if (!localObjectsSet.Contains(localObj))
                        {
                            uniqueCollection.Collection.Add(pathStep.StepFieldObject);
                            localObjectsSet.Add(localObj);
                        }
                    }
                }
                else {
                    throw new GraphViewException("Dedup(local) can only be applied to a list.");
                }

                result.Append(uniqueCollection);
                return result;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    [KnownType(typeof(RangeInBatchOperator))]
    internal class RangeOperator : GraphViewExecutionOperator
    {
        [DataMember]
        protected GraphViewExecutionOperator inputOp;
        [DataMember]
        protected int startIndex;
        //
        // if count is -1, return all the records starting from startIndex
        //
        [DataMember]
        protected int highEnd;
        protected int index;

        internal RangeOperator(GraphViewExecutionOperator inputOp, int startIndex, int count)
        {
            this.inputOp = inputOp;
            this.startIndex = startIndex;
            this.highEnd = count == -1 ? -1 : startIndex + count;
            this.index = 0;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            //
            // Return records in the [startIndex, highEnd)
            //
            while (this.inputOp.State() && (srcRecord = this.inputOp.Next()) != null)
            {
                if (this.highEnd != -1 && this.index >= this.highEnd)
                {
                    break;
                }
                if (this.index < this.startIndex)
                {
                    this.index++;
                    continue;
                }

                this.index++;
                return srcRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.index = 0;
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.index = 0;
            this.Open();
        }
    }

    [DataContract]
    internal class RangeInBatchOperator : RangeOperator
    {
        private RawRecord firstRecordInGroup;

        internal RangeInBatchOperator(
            GraphViewExecutionOperator inputOp,
            int startIndex,
            int count)
            : base(inputOp, startIndex, count) { }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.firstRecordInGroup == null && this.inputOp.State())
                {
                    this.firstRecordInGroup = this.inputOp.Next();
                }

                if (this.firstRecordInGroup == null)
                {
                    this.Close();
                    return null;
                }

                // First record in the group
                if (this.index == 0)
                {
                    if (this.startIndex == 0)
                    {
                        this.index++;
                        return this.firstRecordInGroup;
                    }
                    else
                    {
                        this.index++;
                    }
                }

                RawRecord rec = null;
                // Return a record within [startIndex, highEnd) in the group
                while (this.inputOp.State() &&
                    (rec = this.inputOp.Next()) != null &&
                    rec[0].ToValue == this.firstRecordInGroup[0].ToValue)
                {
                    if (this.index < this.startIndex || (this.highEnd != -1 && this.index >= this.highEnd))
                    {
                        this.index++;
                        continue;
                    }

                    this.index++;
                    return rec;
                }

                // Passes the current group. Reaches a new group. Resets the index.
                this.firstRecordInGroup = rec;
                this.index = 0;
                continue;
            }

            return null;
        }
    }

    [DataContract]
    internal class RangeLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int startIndex;
        //
        // if count is -1, return all the records starting from startIndex
        //
        [DataMember]
        private int count;
        [DataMember]
        private int inputCollectionIndex;

        [DataMember]
        private List<string> populateColumns;
        [DataMember]
        private bool wantSingleObject;

        internal RangeLocalOperator(
            GraphViewExecutionOperator inputOp, 
            int inputCollectionIndex, 
            int startIndex, int count,
            List<string> populateColumns)
        {
            this.inputOp = inputOp;
            this.startIndex = startIndex;
            this.count = count;
            this.inputCollectionIndex = inputCollectionIndex;
            this.populateColumns = populateColumns;
            this.wantSingleObject = this.count == 1;

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (this.inputOp.State() && (srcRecord = this.inputOp.Next()) != null)
            {
                RawRecord newRecord = new RawRecord(srcRecord);
                //
                // Return records in the [runtimeStartIndex, runtimeStartIndex + runtimeCount)
                //
                FieldObject inputObject = srcRecord[inputCollectionIndex];
                FieldObject filteredObject;
                if (inputObject is CollectionField)
                {
                    CollectionField inputCollection = (CollectionField)inputObject;
                    CollectionField newCollectionField = new CollectionField();

                    int runtimeStartIndex = startIndex > inputCollection.Collection.Count ? inputCollection.Collection.Count : startIndex;
                    int runtimeCount = this.count == -1 ? inputCollection.Collection.Count - runtimeStartIndex : this.count;
                    if (runtimeStartIndex + runtimeCount > inputCollection.Collection.Count) {
                        runtimeCount = inputCollection.Collection.Count - runtimeStartIndex;
                    }

                    newCollectionField.Collection = inputCollection.Collection.GetRange(runtimeStartIndex, runtimeCount);
                    if (wantSingleObject) {
                        filteredObject = newCollectionField.Collection.Any() ? newCollectionField.Collection[0] : null;
                    }
                    else {
                        filteredObject = newCollectionField;
                    }
                }
                else if (inputObject is PathField)
                {
                    PathField inputPath = (PathField)inputObject;
                    CollectionField newCollectionField = new CollectionField();

                    int runtimeStartIndex = startIndex > inputPath.Path.Count ? inputPath.Path.Count : startIndex;
                    int runtimeCount = this.count == -1 ? inputPath.Path.Count - runtimeStartIndex : this.count;
                    if (runtimeStartIndex + runtimeCount > inputPath.Path.Count) {
                        runtimeCount = inputPath.Path.Count - runtimeStartIndex;
                    }

                    newCollectionField.Collection =
                        inputPath.Path.GetRange(runtimeStartIndex, runtimeCount)
                            .Cast<PathStepField>()
                            .Select(p => p.StepFieldObject)
                            .ToList();
                    if (wantSingleObject) {
                        filteredObject = newCollectionField.Collection.Any() ? newCollectionField.Collection[0] : null;
                    }
                    else {
                        filteredObject = newCollectionField;
                    }
                }
                //
                // Return records in the [low, high)
                //
                else if (inputObject is MapField)
                {
                    MapField inputMap = (MapField)inputObject;
                    MapField newMap = new MapField();

                    int low = startIndex;
                    int high = this.count == -1 ? inputMap.Count : low + this.count;

                    int index = 0;
                    foreach (EntryField entry in inputMap) {
                        if (index >= low && index < high) {
                            newMap.Add(entry.Key, entry.Value);
                        }
                        if (++index >= high) {
                            break;
                        }
                    }
                    filteredObject = newMap;
                }
                else {
                    filteredObject = inputObject;
                }

                if (filteredObject == null) {
                    continue;
                }
                RawRecord flatRawRecord = filteredObject.FlatToRawRecord(this.populateColumns);
                newRecord.Append(flatRawRecord);
                return newRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    [KnownType(typeof(TailInBatchOperator))]
    internal class TailOperator : GraphViewExecutionOperator
    {
        [DataMember]
        protected GraphViewExecutionOperator inputOp;
        [DataMember]
        protected int lastN;
        protected int count;
        protected List<RawRecord> buffer; 

        internal TailOperator(GraphViewExecutionOperator inputOp, int lastN)
        {
            this.inputOp = inputOp;
            this.lastN = lastN;
            this.count = 0;
            this.buffer = new List<RawRecord>();

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (this.inputOp.State() && (srcRecord = this.inputOp.Next()) != null) {
                buffer.Add(srcRecord);
            }

            //
            // Reutn records from [buffer.Count - lastN, buffer.Count)
            //

            int startIndex = buffer.Count < lastN ? 0 : buffer.Count - lastN;
            int index = startIndex + this.count++;
            while (index < buffer.Count) {
                return buffer[index];
            } 

            this.Close();
            this.buffer.Clear();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.count = 0;
            this.buffer.Clear();
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.count = 0;
            this.buffer = new List<RawRecord>();

            this.Open();
        }
    }

    [DataContract]
    internal class TailInBatchOperator : TailOperator
    {
        private RawRecord firstRecordInGroup;

        internal TailInBatchOperator(
            GraphViewExecutionOperator inputOp,
            int lastN)
            : base(inputOp, lastN) { }

        public override RawRecord Next()
        {
            while (this.State())
            {
                int startIndex = this.buffer.Count < this.lastN ? 0 : this.buffer.Count - this.lastN;
                int index = startIndex + this.count++;
                while (index < this.buffer.Count)
                {
                    return this.buffer[index];
                }
                this.count = 0;
                this.buffer.Clear();

                if (this.firstRecordInGroup == null && this.inputOp.State())
                {
                    this.firstRecordInGroup = this.inputOp.Next();
                }

                if (this.firstRecordInGroup == null)
                {
                    this.Close();
                    return null;
                }

                this.buffer.Add(this.firstRecordInGroup);
                // Collect one group into the buffer
                RawRecord rec = null;
                while (this.inputOp.State() &&
                       (rec = this.inputOp.Next()) != null &&
                       rec[0].ToValue == this.firstRecordInGroup[0].ToValue)
                {
                    this.buffer.Add(rec);
                }

                this.firstRecordInGroup = rec;
                continue;
            }

            return null;
        }
    }

    [DataContract]
    internal class TailLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int lastN;
        [DataMember]
        private int inputCollectionIndex;

        [DataMember]
        private List<string> populateColumns;
        [DataMember]
        private bool wantSingleObject;

        internal TailLocalOperator(
            GraphViewExecutionOperator inputOp, 
            int inputCollectionIndex, int lastN,
            List<string> populateColumns)
        {
            this.inputOp = inputOp;
            this.inputCollectionIndex = inputCollectionIndex;
            this.lastN = lastN;
            this.populateColumns = populateColumns;
            this.wantSingleObject = this.lastN == 1;

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (this.inputOp.State() && (srcRecord = this.inputOp.Next()) != null)
            {
                RawRecord newRecord = new RawRecord(srcRecord);
                //
                // Return records in the [localCollection.Count - lastN, localCollection.Count)
                //
                FieldObject inputObject = srcRecord[inputCollectionIndex];
                FieldObject filteredObject;
                if (inputObject is CollectionField)
                {
                    CollectionField inputCollection = (CollectionField)inputObject;
                    CollectionField newCollection = new CollectionField();

                    int startIndex = inputCollection.Collection.Count < lastN 
                                     ? 0 
                                     : inputCollection.Collection.Count - lastN;
                    int count = startIndex + lastN > inputCollection.Collection.Count
                                     ? inputCollection.Collection.Count - startIndex
                                     : lastN;

                    newCollection.Collection = inputCollection.Collection.GetRange(startIndex, count);
                    if (wantSingleObject) {
                        filteredObject = newCollection.Collection.Any() ? newCollection.Collection[0] : null;
                    }
                    else {
                        filteredObject = newCollection;
                    }
                }
                else if (inputObject is PathField)
                {
                    PathField inputPath = (PathField)inputObject;
                    CollectionField newCollection = new CollectionField();

                    int startIndex = inputPath.Path.Count < lastN
                                     ? 0
                                     : inputPath.Path.Count - lastN;
                    int count = startIndex + lastN > inputPath.Path.Count
                                     ? inputPath.Path.Count - startIndex
                                     : lastN;

                    newCollection.Collection =
                        inputPath.Path.GetRange(startIndex, count)
                            .Cast<PathStepField>()
                            .Select(p => p.StepFieldObject)
                            .ToList();
                    if (wantSingleObject) {
                        filteredObject = newCollection.Collection.Any() ? newCollection.Collection[0] : null;
                    }
                    else {
                        filteredObject = newCollection;
                    }
                }
                //
                // Return records in the [low, inputMap.Count)
                //
                else if (inputObject is MapField)
                {
                    MapField inputMap = inputObject as MapField;
                    MapField newMap = new MapField();
                    int low = inputMap.Count - lastN;

                    int index = 0;
                    foreach (EntryField entry in inputMap) {
                        if (index++ >= low)
                            newMap.Add(entry.Key, entry.Value);
                    }
                    filteredObject = newMap;
                }
                else {
                    filteredObject = inputObject;
                }

                if (filteredObject == null) {
                    continue;
                }
                RawRecord flatRawRecord = filteredObject.FlatToRawRecord(this.populateColumns);
                newRecord.Append(flatRawRecord);
                return newRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class InjectOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;

        [DataMember]
        private readonly int inputRecordColumnsCount;
        [DataMember]
        private readonly int injectColumnIndex;

        [DataMember]
        private readonly bool isList;
        [DataMember]
        private readonly string defaultProjectionKey;

        [DataMember]
        private readonly List<ScalarFunction> injectValues;

        private bool hasInjected;

        public InjectOperator(
            GraphViewExecutionOperator inputOp,
            int inputRecordColumnsCount,
            int injectColumnIndex,
            List<ScalarFunction> injectValues,
            bool isList,
            string defalutProjectionKey
            )
        {
            this.inputOp = inputOp;
            this.inputRecordColumnsCount = inputRecordColumnsCount;
            this.injectColumnIndex = injectColumnIndex;
            this.injectValues = injectValues;
            this.isList = isList;
            this.defaultProjectionKey = defalutProjectionKey;
            this.hasInjected = false;

            this.Open();
        }

        public override RawRecord Next()
        {
            if (!this.hasInjected)
            {
                this.hasInjected = true;
                RawRecord result = new RawRecord();

                if (isList)
                {
                    List<FieldObject> collection = new List<FieldObject>();
                    foreach (ScalarFunction injectValueFunc in this.injectValues)
                    {
                        Dictionary<string, FieldObject> compositeFieldObjects = new Dictionary<string, FieldObject>();
                        compositeFieldObjects.Add(defaultProjectionKey, injectValueFunc.Evaluate(null));
                        collection.Add(new CompositeField(compositeFieldObjects, defaultProjectionKey));
                    }

                    //
                    // g.Inject()
                    //
                    if (this.inputRecordColumnsCount == 0) {
                        result.Append(new CollectionField(collection));
                    }
                    else
                    {
                        for (int columnIndex = 0; columnIndex < this.inputRecordColumnsCount; columnIndex++) {
                            if (columnIndex == this.injectColumnIndex)
                                result.Append(new CollectionField(collection));
                            else
                                result.Append((FieldObject)null);
                        }
                    }

                    return result;
                }
                else
                {
                    //
                    // g.Inject()
                    //
                    if (this.inputRecordColumnsCount == 0) {
                        result.Append(this.injectValues[0].Evaluate(null));
                    }
                    else
                    {
                        for (int columnIndex = 0; columnIndex < this.inputRecordColumnsCount; columnIndex++) {
                            if (columnIndex == this.injectColumnIndex)
                                result.Append(this.injectValues[0].Evaluate(null));
                            else
                                result.Append((FieldObject)null);
                        }
                    }

                    return result;
                }
            }


            RawRecord r = null;
            //
            // For the g.Inject() case, Inject operator itself is the first operator, and its inputOp is null
            //
            if (this.inputOp != null) {
                r = this.inputOp.State() ? this.inputOp.Next() : null;
            }

            if (r == null) {
                this.Close();
            }

            return r;
        }

        public override void ResetState()
        {
            this.inputOp?.ResetState();
            this.hasInjected = false;
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.hasInjected = false;
        }
    }

    [DataContract]
    internal class AggregateOperator : GraphViewExecutionOperator
    {
        private CollectionFunction aggregateState;
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private ScalarFunction getAggregateObjectFunction;

        private Queue<RawRecord> outputBuffer;
        [DataMember]
        private readonly string storedName;

        public AggregateOperator(GraphViewExecutionOperator inputOp, ScalarFunction getTargetFieldFunction, 
            CollectionFunction aggregateState, string storedName)
        {
            this.aggregateState = aggregateState;
            this.inputOp = inputOp;
            this.getAggregateObjectFunction = getTargetFieldFunction;
            this.outputBuffer = new Queue<RawRecord>();
            this.storedName = storedName;
            Open();
        }

        public override RawRecord Next()
        {
            RawRecord r = null;
            while (inputOp.State() && (r = inputOp.Next()) != null)
            {
                RawRecord result = new RawRecord(r);

                FieldObject aggregateObject = getAggregateObjectFunction.Evaluate(r);

                if (aggregateObject == null)
                    throw new GraphViewException("The provided traversal or property name in Aggregate does not map to a value.");

                aggregateState.Accumulate(aggregateObject);

                result.Append(aggregateState.CollectionField);

                outputBuffer.Enqueue(result);
            }

            if (outputBuffer.Count <= 1) Close();
            if (outputBuffer.Count != 0) return outputBuffer.Dequeue();
            return null;
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.outputBuffer = new Queue<RawRecord>();
            this.aggregateState = (CollectionFunction)SerializationData.SideEffectStates[this.storedName];
        }
    }

    [DataContract]
    internal class StoreOperator : GraphViewExecutionOperator
    {
        private CollectionFunction storeState;
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private ScalarFunction getStoreObjectFunction;
        [DataMember]
        private readonly string storedName;

        public StoreOperator(GraphViewExecutionOperator inputOp, ScalarFunction getTargetFieldFunction, 
            CollectionFunction storeState, string storedName)
        {
            this.storeState = storeState;
            this.inputOp = inputOp;
            this.getStoreObjectFunction = getTargetFieldFunction;
            this.storedName = storedName;
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

                RawRecord result = new RawRecord(r);

                FieldObject storeObject = getStoreObjectFunction.Evaluate(r);

                if (storeObject == null)
                    throw new GraphViewException("The provided traversal or property name in Store does not map to a value.");

                storeState.Accumulate(storeObject);

                result.Append(storeState.CollectionField);

                if (!inputOp.State())
                {
                    Close();
                }
                return result;
            }

            return null;
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.storeState = (CollectionFunction)SerializationData.SideEffectStates[this.storedName];
        }
    }


    //
    // Note: our BarrierOperator's semantics is not the same the one's in Gremlin
    //
    [DataContract]
    internal class BarrierOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator _inputOp;
        private Queue<RawRecord> _outputBuffer;
        [DataMember]
        private int _outputBufferSize;

        public BarrierOperator(GraphViewExecutionOperator inputOp, int outputBufferSize = -1)
        {
            _inputOp = inputOp;
            _outputBuffer = new Queue<RawRecord>();
            _outputBufferSize = outputBufferSize;
            Open();
        }
          
        public override RawRecord Next()
        {
            while (_outputBuffer.Any()) {
                return _outputBuffer.Dequeue();
            }

            RawRecord record;
            while ((_outputBufferSize == -1 || _outputBuffer.Count <= _outputBufferSize) 
                    && _inputOp.State() 
                    && (record = _inputOp.Next()) != null)
            {
                _outputBuffer.Enqueue(record);
            }

            if (_outputBuffer.Count <= 1) Close();
            if (_outputBuffer.Count != 0) return _outputBuffer.Dequeue();
            return null;
        }

        public override void ResetState()
        {
            _inputOp.ResetState();
            _outputBuffer.Clear();
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this._outputBuffer = new Queue<RawRecord>();
        }
    }

    [DataContract]
    internal class ProjectByOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private List<Tuple<string, ScalarFunction>> projectList;

        internal ProjectByOperator(GraphViewExecutionOperator pInputOperator)
        {
            this.inputOp = pInputOperator;
            this.projectList = new List<Tuple<string, ScalarFunction>>();
            this.Open();
        }

        public void AddProjectBy(string key, ScalarFunction byFunction)
        {
            this.projectList.Add(new Tuple<string, ScalarFunction>(key, byFunction));
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                MapField projectMap = new MapField();

                foreach (Tuple<string, ScalarFunction> tuple in this.projectList)
                {
                    string projectKey = tuple.Item1;
                    ScalarFunction byFunction = tuple.Item2;
                    FieldObject projectValue = byFunction.Evaluate(currentRecord);

                    if (projectValue == null)
                        throw new GraphViewException(
                            $"The provided traverser of key \"{projectKey}\" does not map to a value.");

                    projectMap.Add(new StringField(projectKey), projectValue);
                }

                RawRecord result = new RawRecord(currentRecord);
                result.Append(projectMap);

                return result;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class PropertyKeyOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int propertyFieldIndex;

        public PropertyKeyOperator(GraphViewExecutionOperator inputOp, int propertyFieldIndex)
        {
            this.inputOp = inputOp;
            this.propertyFieldIndex = propertyFieldIndex;
            this.Open();
        }


        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (inputOp.State() && (currentRecord = inputOp.Next()) != null)
            {
                PropertyField p = currentRecord[this.propertyFieldIndex] as PropertyField;
                if (p == null)
                    continue;

                RawRecord result = new RawRecord(currentRecord);
                result.Append(new StringField(p.PropertyName));

                return result;
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class PropertyValueOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int propertyFieldIndex;

        public PropertyValueOperator(GraphViewExecutionOperator inputOp, int propertyFieldIndex)
        {
            this.inputOp = inputOp;
            this.propertyFieldIndex = propertyFieldIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (inputOp.State() && (currentRecord = inputOp.Next()) != null)
            {
                PropertyField p = currentRecord[this.propertyFieldIndex] as PropertyField;
                if (p == null)
                    continue;

                RawRecord result = new RawRecord(currentRecord);
                result.Append(new StringField(p.PropertyValue, p.JsonDataType));

                return result;
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class CountLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int objectIndex;

        public CountLocalOperator(GraphViewExecutionOperator inputOp, int objectIndex)
        {
            this.inputOp = inputOp;
            this.objectIndex = objectIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                RawRecord result = new RawRecord(currentRecord);
                FieldObject obj = currentRecord[this.objectIndex];
                Debug.Assert(obj != null, "The input of the CountLocalOperator should not be null.");

                if (obj is CollectionField)
                    result.Append(new StringField(((CollectionField)obj).Collection.Count.ToString(), JsonDataType.Long));
                else if (obj is PathField)
                    result.Append(new StringField(((PathField)obj).Path.Count.ToString(), JsonDataType.Long));
                else if (obj is MapField)
                    result.Append(new StringField(((MapField)obj).Count.ToString(), JsonDataType.Long));
                else if (obj is TreeField)
                    result.Append(new StringField(((TreeField)obj).Children.Count.ToString(), JsonDataType.Long));
                else
                    result.Append(new StringField("1", JsonDataType.Long));

                return result;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class SumLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int objectIndex;

        public SumLocalOperator(GraphViewExecutionOperator inputOp, int objectIndex)
        {
            this.inputOp = inputOp;
            this.objectIndex = objectIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                FieldObject obj = currentRecord[this.objectIndex];
                Debug.Assert(obj != null, "The input of the SumLocalOperator should not be null.");

                double sum = 0.0;
                double current;

                if (obj is CollectionField)
                {
                    foreach (FieldObject fieldObject in ((CollectionField)obj).Collection)
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        sum += current;
                    }
                }
                else if (obj is PathField)
                {
                    foreach (FieldObject fieldObject in ((PathField)obj).Path)
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        sum += current;
                    }
                }
                else {
                    sum = double.TryParse(obj.ToValue, out current) ? current : double.NaN;
                }

                RawRecord result = new RawRecord(currentRecord);
                result.Append(new StringField(sum.ToString(CultureInfo.InvariantCulture), JsonDataType.Double));

                return result;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class MaxLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int objectIndex;

        public MaxLocalOperator(GraphViewExecutionOperator inputOp, int objectIndex)
        {
            this.inputOp = inputOp;
            this.objectIndex = objectIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                FieldObject obj = currentRecord[this.objectIndex];
                Debug.Assert(obj != null, "The input of the MaxLocalOperator should not be null.");

                double max = double.MinValue;
                double current;

                if (obj is CollectionField)
                {
                    foreach (FieldObject fieldObject in ((CollectionField)obj).Collection)
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        if (max < current)
                            max = current;
                    }
                }
                else if (obj is PathField)
                {
                    foreach (PathStepField fieldObject in ((PathField)obj).Path.Cast<PathStepField>())
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        if (max < current)
                            max = current;
                    }
                }
                else {
                    max = double.TryParse(obj.ToValue, out current) ? current : double.NaN;
                }

                RawRecord result = new RawRecord(currentRecord);
                result.Append(new StringField(max.ToString(CultureInfo.InvariantCulture), JsonDataType.Double));

                return result;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class MinLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int objectIndex;

        public MinLocalOperator(GraphViewExecutionOperator inputOp, int objectIndex)
        {
            this.inputOp = inputOp;
            this.objectIndex = objectIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                FieldObject obj = currentRecord[this.objectIndex];
                Debug.Assert(obj != null, "The input of the MinLocalOperator should not be null.");

                double min = double.MaxValue;
                double current;

                if (obj is CollectionField)
                {
                    foreach (FieldObject fieldObject in ((CollectionField)obj).Collection)
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        if (current < min)
                            min = current;
                    }
                }
                else if (obj is PathField)
                {
                    foreach (PathStepField fieldObject in ((PathField)obj).Path.Cast<PathStepField>())
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        if (current < min)
                            min = current;
                    }
                }
                else {
                    min = double.TryParse(obj.ToValue, out current) ? current : double.NaN;
                }

                RawRecord result = new RawRecord(currentRecord);
                result.Append(new StringField(min.ToString(CultureInfo.InvariantCulture), JsonDataType.Double));

                return result;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class MeanLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int objectIndex;

        public MeanLocalOperator(GraphViewExecutionOperator inputOp, int objectIndex)
        {
            this.inputOp = inputOp;
            this.objectIndex = objectIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                FieldObject obj = currentRecord[this.objectIndex];
                Debug.Assert(obj != null, "The input of the MeanLocalOperator should not be null.");

                double sum = 0.0;
                long count = 0;
                double current;

                if (obj is CollectionField)
                {
                    foreach (FieldObject fieldObject in ((CollectionField)obj).Collection)
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        sum += current;
                        count++;
                    }
                }
                else if (obj is PathField)
                {
                    foreach (PathStepField fieldObject in ((PathField)obj).Path.Cast<PathStepField>())
                    {
                        if (!double.TryParse(fieldObject.ToValue, out current))
                            throw new GraphViewException("The element of the local object cannot be cast to a number");

                        sum += current;
                        count++;
                    }
                }
                else
                {
                    count = 1;
                    sum = double.TryParse(obj.ToValue, out current) ? current : double.NaN;
                }

                RawRecord result = new RawRecord(currentRecord);
                result.Append(new StringField((sum / count).ToString(CultureInfo.InvariantCulture), JsonDataType.Double));

                return result;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class SimplePathOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int pathIndex;

        public SimplePathOperator(GraphViewExecutionOperator inputOp, int pathIndex)
        {
            this.inputOp = inputOp;
            this.pathIndex = pathIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                RawRecord result = new RawRecord(currentRecord);
                PathField path = currentRecord[pathIndex] as PathField;

                Debug.Assert(path != null, "The input of the simplePath filter should be a PathField generated by path().");

                HashSet<Object> intermediateStepSet = new HashSet<Object>();
                bool isSimplePath = true;

                foreach (PathStepField step in path.Path.Cast<PathStepField>())
                {
                    Object stepObj = step.ToObject();
                    if (intermediateStepSet.Contains(stepObj))
                    {
                        isSimplePath = false;
                        break;
                    }
                        
                    intermediateStepSet.Add(stepObj);
                }

                if (isSimplePath) {
                    return result;
                }
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class CyclicPathOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private GraphViewExecutionOperator inputOp;
        [DataMember]
        private int pathIndex;

        public CyclicPathOperator(GraphViewExecutionOperator inputOp, int pathIndex)
        {
            this.inputOp = inputOp;
            this.pathIndex = pathIndex;
            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord currentRecord;

            while (this.inputOp.State() && (currentRecord = this.inputOp.Next()) != null)
            {
                RawRecord result = new RawRecord(currentRecord);
                PathField path = currentRecord[pathIndex] as PathField;

                Debug.Assert(path != null, "The input of the cyclicPath filter should be a CollectionField generated by path().");

                HashSet<Object> intermediateStepSet = new HashSet<Object>();
                bool isCyclicPath = false;

                foreach (PathStepField step in path.Path.Cast<PathStepField>())
                {
                    Object stepObj = step.ToObject();
                    if (intermediateStepSet.Contains(stepObj))
                    {
                        isCyclicPath = true;
                        break;
                    }

                    intermediateStepSet.Add(stepObj);
                }

                if (isCyclicPath) {
                    return result;
                }
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class CoinOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly double _probability;
        [DataMember]
        private readonly GraphViewExecutionOperator _inputOp;
        private Random _random;

        public CoinOperator(
            GraphViewExecutionOperator inputOp,
            double probability)
        {
            this._inputOp = inputOp;
            this._probability = probability;
            this._random = new Random();

            Open();
        }

        public override RawRecord Next()
        {
            RawRecord current = null;
            while (this._inputOp.State() && (current = this._inputOp.Next()) != null) {
                if (this._random.NextDouble() <= this._probability) {
                    return current;
                }
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            this._inputOp.ResetState();
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this._random = new Random();
        }
    }

    [DataContract]
    [KnownType(typeof(SampleInBatchOperator))]
    internal class SampleOperator : GraphViewExecutionOperator
    {
        [DataMember]
        protected readonly GraphViewExecutionOperator inputOp;
        [DataMember]
        protected readonly long amountToSample;
        [DataMember]
        protected readonly ScalarFunction byFunction;  // Can be null if no "by" step

        protected Random random;

        protected List<RawRecord> inputRecords;
        protected List<RawRecord> sampleRecords;
        protected List<double> inputProperties;
        protected int nextIndex;

        public SampleOperator(
            GraphViewExecutionOperator inputOp,
            long amountToSample,
            ScalarFunction byFunction)
        {
            this.inputOp = inputOp;
            this.amountToSample = amountToSample;
            this.byFunction = byFunction;  // Can be null if no "by" step
            this.random = new Random();

            this.inputRecords = new List<RawRecord>();
            this.inputProperties = new List<double>();
            this.nextIndex = 0;
            Open();
        }

        public override RawRecord Next()
        {
            if (this.nextIndex == 0)
            {
                RawRecord current;
                while (this.inputOp.State() && (current = this.inputOp.Next()) != null)
                {
                    this.inputRecords.Add(current);
                    if (this.byFunction != null)
                    {
                        this.inputProperties.Add(double.Parse(this.byFunction.Evaluate(current).ToValue));
                    }
                }

                if (this.byFunction != null)
                {
                    SamplingAlgorithm.WeightedReservoirSample(this.inputRecords, this.inputProperties, (int)amountToSample, random, out sampleRecords);
                }
                else
                {
                    SamplingAlgorithm.ReservoirSample(this.inputRecords, (int)amountToSample, random, out this.sampleRecords);
                }
            }

            if (this.nextIndex < this.amountToSample && this.nextIndex < this.sampleRecords.Count) {
                return this.sampleRecords[this.nextIndex++];
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();

            this.inputRecords.Clear();
            this.inputProperties.Clear();
            this.nextIndex = 0;
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.random = new Random();
            this.inputRecords = new List<RawRecord>();
            this.inputProperties = new List<double>();
            this.nextIndex = 0;
        }
    }

    [DataContract]
    internal class SampleInBatchOperator : SampleOperator
    {
        private RawRecord firstRecordInGroup;

        internal SampleInBatchOperator(
            GraphViewExecutionOperator inputOp,
            long amountToSample,
            ScalarFunction byFunction)
            : base(inputOp, amountToSample, byFunction) { }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.sampleRecords.Any())
                {
                    if (this.nextIndex < this.amountToSample && this.nextIndex < this.sampleRecords.Count)
                    {
                        return this.sampleRecords[this.nextIndex++];
                    }
                    else
                    {
                        this.Close();
                        return null;
                    }
                }

                this.nextIndex = 0;
                this.inputRecords.Clear();
                this.inputProperties.Clear();
                this.sampleRecords.Clear();

                if (this.firstRecordInGroup == null && this.inputOp.State())
                {
                    this.firstRecordInGroup = this.inputOp.Next();
                }

                if (this.firstRecordInGroup == null)
                {
                    this.Close();
                    return null;
                }

                this.inputRecords.Add(this.firstRecordInGroup);
                if (this.byFunction != null)
                {
                    this.inputProperties.Add(double.Parse(this.byFunction.Evaluate(this.firstRecordInGroup).ToValue));
                }

                RawRecord rec = null;
                while (this.inputOp.State() &&
                    (rec = this.inputOp.Next()) != null &&
                    rec[0].ToValue == this.firstRecordInGroup[0].ToValue)
                {
                    this.inputRecords.Add(this.firstRecordInGroup);
                    if (this.byFunction != null)
                    {
                        this.inputProperties.Add(double.Parse(this.byFunction.Evaluate(this.firstRecordInGroup).ToValue));
                    }
                }

                if (this.byFunction != null)
                {
                    SamplingAlgorithm.WeightedReservoirSample(this.inputRecords, this.inputProperties, (int)amountToSample, random, out sampleRecords);
                }
                else
                {
                    SamplingAlgorithm.ReservoirSample(this.inputRecords, (int)amountToSample, random, out this.sampleRecords);
                }

                // Passes the current group. Reaches a new group. Resets the index.
                this.firstRecordInGroup = rec;
            }

            return null;
        }
    }

    [DataContract]
    internal class SampleLocalOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly GraphViewExecutionOperator inputOp;
        [DataMember]
        private readonly long amountToSample;

        private Random random;

        [DataMember]
        private readonly int inputObjectIndex;
        [DataMember]
        private List<string> populateColumns;

        public SampleLocalOperator(GraphViewExecutionOperator inputOp, int inputObjectIndex, long amountToSample, List<string> populateColumns)
        {
            this.inputOp = inputOp;
            this.amountToSample = amountToSample;
            this.inputObjectIndex = inputObjectIndex;
            this.random = new Random();
            this.populateColumns = populateColumns;
            Open();
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord;

            while (this.inputOp.State() && (srcRecord = this.inputOp.Next()) != null)
            {
                RawRecord newRecord = new RawRecord(srcRecord);
                FieldObject inputObject = srcRecord[this.inputObjectIndex];
                FieldObject sampleObject;
                int count = 0;

                if (inputObject is CollectionField)
                {
                    CollectionField inputCollection = (CollectionField)inputObject;
                    List<FieldObject> sampleList;
                    SamplingAlgorithm.ReservoirSample(inputCollection.Collection, (int)amountToSample, random, out sampleList);
                    CollectionField newCollection = new CollectionField() { Collection = sampleList };
                    sampleObject = newCollection;
                }
                else if (inputObject is PathField)
                {
                    PathField inputPath = (PathField)inputObject;
                    List<FieldObject> sampleList;
                    SamplingAlgorithm.ReservoirSample(inputPath.Path, (int)amountToSample, random, out sampleList);
                    CollectionField newCollection = new CollectionField() { Collection = sampleList };
                    sampleObject = newCollection;
                }
                else if (inputObject is MapField)
                {
                    MapField inputMap = inputObject as MapField;
                    List<FieldObject> keys = new List<FieldObject>();
                    foreach (EntryField entry in inputMap)
                    {
                        keys.Add(entry.Key);
                    }

                    List<FieldObject> sampleList;
                    SamplingAlgorithm.ReservoirSample(keys, (int)amountToSample, random, out sampleList);

                    MapField newMap = new MapField();
                    foreach (FieldObject key in sampleList)
                    {
                        newMap.Add(key, inputMap[key]);
                    }
                    sampleObject = newMap;
                }
                else
                {
                    sampleObject = inputObject;
                }

                if (sampleObject == null)
                {
                    continue;
                }
                RawRecord flatRawRecord = sampleObject.FlatToRawRecord(this.populateColumns);
                newRecord.Append(flatRawRecord);
                return newRecord;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.random = new Random();
        }
    }

    [DataContract]
    internal class Decompose1Operator : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly GraphViewExecutionOperator inputOp;
        [DataMember]
        private readonly int decomposeTargetIndex;
        [DataMember]
        private readonly List<string> populateColumns;
        [DataMember]
        private readonly string tableDefaultColumnName;

        public Decompose1Operator(
            GraphViewExecutionOperator inputOp,
            int decomposeTargetIndex,
            List<string> populateColumns,
            string tableDefaultColumnName)
        {
            this.inputOp = inputOp;
            this.decomposeTargetIndex = decomposeTargetIndex;
            this.populateColumns = populateColumns;
            this.tableDefaultColumnName = tableDefaultColumnName;

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord inputRecord = null;
            while (this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
            {
                FieldObject inputObj = inputRecord[this.decomposeTargetIndex];
                CompositeField compose1Obj = inputRecord[this.decomposeTargetIndex] as CompositeField;

                RawRecord r = new RawRecord(inputRecord);
                if (compose1Obj != null)
                {
                    foreach (string populateColumn in this.populateColumns) {
                        r.Append(compose1Obj[populateColumn]);
                    }
                }
                else {
                    foreach (string columnName in this.populateColumns) {
                        if (columnName.Equals(this.tableDefaultColumnName)) {
                            r.Append(inputObj);
                        }
                        else {
                            r.Append((FieldObject)null);
                        }
                    }
                }

                return r;
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    internal class SelectColumnOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly GraphViewExecutionOperator inputOp;
        [DataMember]
        private readonly int inputTargetIndex;
        [DataMember]
        private readonly List<string> populateColumns;

        //
        // true, select(keys)
        // false, select(values)
        //
        [DataMember]
        private readonly bool isSelectKeys;

        public SelectColumnOperator(
            GraphViewExecutionOperator inputOp,
            int inputTargetIndex,
            bool isSelectKeys,
            List<string> populateColumns)
        {
            this.inputOp = inputOp;
            this.inputTargetIndex = inputTargetIndex;
            this.isSelectKeys = isSelectKeys;
            this.populateColumns = populateColumns;

            this.Open();
        }

        public override RawRecord Next()
        {
            RawRecord inputRecord = null;
            while (this.inputOp.State() && (inputRecord = this.inputOp.Next()) != null)
            {
                FieldObject selectObj = inputRecord[this.inputTargetIndex];
                RawRecord r = new RawRecord(inputRecord);

                if (selectObj is MapField)
                {
                    MapField inputMap = (MapField)selectObj;
                    List<FieldObject> columns = new List<FieldObject>();

                    foreach (EntryField entry in inputMap) {
                        columns.Add(this.isSelectKeys ? entry.Key : entry.Value);
                    }

                    foreach (string rawRecordColumnName in this.populateColumns)
                    {
                        if (rawRecordColumnName.Equals(GremlinKeyword.TableDefaultColumnName))
                        {
                            r.Append(new CollectionField(columns));
                        }
                        else
                        {
                            r.Append((FieldObject)null);
                        }
                    }

                    return r;
                }
                else if (selectObj is EntryField)
                {
                    EntryField inputEntry = (EntryField) selectObj;
                    CompositeField result = (CompositeField)(this.isSelectKeys ? inputEntry.Key : inputEntry.Value);
                    foreach (string rawRecordColumnName in this.populateColumns)
                    {
                        r.Append(result[rawRecordColumnName]);
                    }
                    return r;
                }
                else if (selectObj is PathField)
                {
                    PathField inputPath = (PathField)selectObj;
                    List<FieldObject> columns = new List<FieldObject>();

                    foreach (PathStepField pathStep in inputPath.Path.Cast<PathStepField>())
                    {
                        if (this.isSelectKeys)
                        {
                            List<FieldObject> labels = new List<FieldObject>();
                            foreach (string label in pathStep.Labels) {
                                labels.Add(new StringField(label));
                            }
                            columns.Add(new CollectionField(labels));
                        } else {
                            columns.Add(pathStep.StepFieldObject);
                        }
                    }

                    foreach (string rawRecordColumnName in this.populateColumns)
                    {
                        if (rawRecordColumnName.Equals(GremlinKeyword.TableDefaultColumnName))
                        {
                            r.Append(new CollectionField(columns));
                        }
                        else
                        {
                            r.Append((FieldObject)null);
                        }
                    }

                    return r;
                }
                else if (selectObj is TreeField)
                {
                    TreeField inputTree = (TreeField)selectObj;
                    List<FieldObject> columns = new List<FieldObject>();

                    foreach (var child in inputTree.Children)
                    {
                        if (this.isSelectKeys)
                        {
                            columns.Add(child.Key);
                        }
                        else
                        {
                            columns.Add(child.Value);
                        }
                    }

                    foreach (string rawRecordColumnName in this.populateColumns)
                    {
                        if (rawRecordColumnName.Equals(GremlinKeyword.TableDefaultColumnName))
                        {
                            r.Append(new CollectionField(columns));
                        }
                        else
                        {
                            r.Append((FieldObject)null);
                        }
                    }

                    return r;
                }
                throw new GraphViewException(string.Format("The provided object does not have acessible {0}.",
                    this.isSelectKeys ? "keys" : "values"));
            }

            this.Close();
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }
    }

    [DataContract]
    [KnownType(typeof(SelectOperator))]
    [KnownType(typeof(SelectOneOperator))]
    internal abstract class SelectBaseOperator : GraphViewExecutionOperator
    {
        [DataMember]
        protected readonly GraphViewExecutionOperator inputOp;

        protected Dictionary<string, IAggregateFunction> sideEffectStates;

        [DataMember]
        protected readonly int inputObjectIndex;
        [DataMember]
        protected readonly int pathIndex;

        [DataMember]
        protected readonly GremlinKeyword.Pop pop;
        [DataMember]
        protected readonly string tableDefaultColumnName;

        protected SelectBaseOperator(
            GraphViewExecutionOperator inputOp,
            Dictionary<string, IAggregateFunction> sideEffectStates,
            int inputObjectIndex,
            int pathIndex,
            GremlinKeyword.Pop pop,
            string tableDefaultColumnName)
        {
            this.inputOp = inputOp;
            this.sideEffectStates = sideEffectStates;
            this.inputObjectIndex = inputObjectIndex;
            this.pathIndex = pathIndex;

            this.pop = pop;
            this.tableDefaultColumnName = tableDefaultColumnName;
        }

        protected FieldObject GetSelectObject(RawRecord inputRec, string label)
        {
            MapField inputMap = inputRec[this.inputObjectIndex] as MapField;
            PathField path = inputRec[this.pathIndex] as PathField;

            StringField labelStringField = new StringField(label);

            IAggregateFunction globalSideEffectObject;
            FieldObject selectObject = null;

            if (this.sideEffectStates.TryGetValue(label, out globalSideEffectObject))
            {
                Dictionary<string, FieldObject> compositeFieldObject = new Dictionary<string, FieldObject>();
                compositeFieldObject.Add(this.tableDefaultColumnName, globalSideEffectObject.Terminate());
                selectObject = new CompositeField(compositeFieldObject, this.tableDefaultColumnName);
            }
            else if (inputMap != null && inputMap.ContainsKey(labelStringField)) {
                selectObject = inputMap[labelStringField];
            }
            else
            {
                Debug.Assert(path != null);
                List<FieldObject> selectObjects = new List<FieldObject>();

                if (this.pop == GremlinKeyword.Pop.First) {
                    foreach (PathStepField step in path.Path.Cast<PathStepField>()) {
                        if (step.Labels.Contains(label)) {
                            selectObjects.Add(step.StepFieldObject);
                            break;
                        }
                    }
                }
                else if (this.pop == GremlinKeyword.Pop.Last) {
                    for (int reverseIndex = path.Path.Count - 1; reverseIndex >= 0; reverseIndex--) {
                        PathStepField step = (PathStepField)path.Path[reverseIndex];
                        if (step.Labels.Contains(label)) {
                            selectObjects.Add(step.StepFieldObject);
                            break;
                        }
                    }
                }
                //
                // this.pop == Pop.All
                //
                else {
                    foreach (PathStepField step in path.Path.Cast<PathStepField>()) {
                        if (step.Labels.Contains(label)) {
                            selectObjects.Add(step.StepFieldObject);
                        }
                    }
                }

                if (selectObjects.Count == 1) {
                    selectObject = selectObjects[0];
                }
                else if (selectObjects.Count > 1)
                {
                    Dictionary<string, FieldObject> compositeFieldObject = new Dictionary<string, FieldObject>();
                    compositeFieldObject.Add(this.tableDefaultColumnName, new CollectionField(selectObjects));
                    selectObject = new CompositeField(compositeFieldObject, this.tableDefaultColumnName);
                }
            }

            return selectObject;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.sideEffectStates = SerializationData.SideEffectStates;
        }
    }

    [DataContract]
    internal class SelectOperator : SelectBaseOperator
    {
        [DataMember]
        private readonly List<string> selectLabels;
        [DataMember]
        private readonly List<ScalarFunction> byFuncList;

        public SelectOperator(
            GraphViewExecutionOperator inputOp,
            Dictionary<string, IAggregateFunction> sideEffectStates,
            int inputObjectIndex,
            int pathIndex,
            GremlinKeyword.Pop pop,
            List<string> selectLabels,
            List<ScalarFunction> byFuncList,
            string tableDefaultColumnName)
            : base(inputOp, sideEffectStates, inputObjectIndex, pathIndex, pop, tableDefaultColumnName)
        {
            this.selectLabels = selectLabels;
            this.byFuncList = byFuncList;

            this.Open();
        }

        private FieldObject GetProjectionResult(FieldObject selectObject, ref int activeByFuncIndex)
        {
            FieldObject projectionResult;

            if (this.byFuncList.Count == 0) {
                projectionResult = selectObject;
            }
            else
            {
                RawRecord initCompose1Record = new RawRecord();
                initCompose1Record.Append(selectObject);
                projectionResult = this.byFuncList[activeByFuncIndex++ % this.byFuncList.Count].Evaluate(initCompose1Record);

                if (projectionResult == null) {
                    throw new GraphViewException("The provided traversal or property name of path() does not map to a value.");
                }
            }

            return projectionResult;
        }

        public override RawRecord Next()
        {
            RawRecord inputRec;
            while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
            {
                int activeByFuncIndex = 0;

                MapField selectMap = new MapField();

                bool allLabelCanBeSelected = true;
                foreach (string label in this.selectLabels)
                {
                    FieldObject selectObject = this.GetSelectObject(inputRec, label);

                    if (selectObject == null)
                    {
                        allLabelCanBeSelected = false;
                        break;
                    }

                    selectMap.Add(new StringField(label), this.GetProjectionResult(selectObject, ref activeByFuncIndex));
                }

                if (!allLabelCanBeSelected) {
                    continue;
                }

                RawRecord r = new RawRecord(inputRec);
                r.Append(selectMap);
                return r;
            }

            this.Close();
            return null;
        }
    }

    [DataContract]
    internal class SelectOneOperator : SelectBaseOperator
    {
        [DataMember]
        private readonly string selectLabel;
        [DataMember]
        private readonly ScalarFunction byFunc;

        [DataMember]
        private readonly List<string> populateColumns;

        public SelectOneOperator(
            GraphViewExecutionOperator inputOp,
            Dictionary<string, IAggregateFunction> sideEffectStates,
            int inputObjectIndex,
            int pathIndex,
            GremlinKeyword.Pop pop,
            string selectLabel,
            ScalarFunction byFunc,
            List<string> populateColumns,
            string tableDefaultColumnName)
            : base(inputOp, sideEffectStates, inputObjectIndex, pathIndex, pop, tableDefaultColumnName)
        {
            this.selectLabel = selectLabel;
            this.byFunc = byFunc;
            this.populateColumns = populateColumns;

            this.Open();
        }

        private FieldObject GetProjectionResult(FieldObject selectObject)
        {
            FieldObject projectionResult;

            RawRecord initCompose1Record = new RawRecord();
            initCompose1Record.Append(selectObject);
            projectionResult = this.byFunc.Evaluate(initCompose1Record);

            if (projectionResult == null) {
                throw new GraphViewException("The provided traversal or property name of path() does not map to a value.");
            }

            return projectionResult;
        }

        public override RawRecord Next()
        {
            RawRecord inputRec;
            while (this.inputOp.State() && (inputRec = this.inputOp.Next()) != null)
            {
                FieldObject selectObject = this.GetSelectObject(inputRec, this.selectLabel);

                if (selectObject == null) {
                    continue;
                }

                CompositeField projectionResult = this.GetProjectionResult(selectObject) as CompositeField;
                Debug.Assert(projectionResult != null, "projectionResult is Compose1Field.");

                RawRecord r = new RawRecord(inputRec);
                foreach (string columnName in this.populateColumns) {
                    r.Append(projectionResult[columnName]);
                }

                return r;
            }

            this.Close();
            return null;
        }
    }

    [DataContract]
    internal class AdjacencyListDecoder : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly GraphViewExecutionOperator inputOp;
        [DataMember]
        private readonly int startVertexIndex;

        [DataMember]
        private readonly bool crossApplyForwardAdjacencyList;
        [DataMember]
        private readonly bool crossApplyBackwardAdjacencyList;

        [DataMember]
        private readonly BooleanFunction edgePredicate;
        [DataMember]
        private readonly List<string> projectedFields;

        [DataMember]
        private readonly bool isStartVertexTheOriginVertex;

        private Queue<RawRecord> outputBuffer;
        private GraphViewCommand command;

        [DataMember]
        private readonly int batchSize;
        // RawRecord: the input record with the lazy adjacency list
        // string: the Id of the vertex of the adjacency list to be decoded
        private Queue<Tuple<RawRecord, string>> lazyAdjacencyListBatch;

        private Queue<RawRecord> inputRecordsBuffer;

        /// <summary>
        /// The length of a record produced by this decoder operator.
        /// If an input record has the same length, meaning the adjacency list has 
        /// been decoded by a prior operator, i.e., FetchNodeOp or TraversalOp,
        /// it is bypassed to the next operator.
        /// </summary>
        [DataMember]
        private readonly int outputRecordLength;

        public AdjacencyListDecoder(
            GraphViewExecutionOperator inputOp,
            int startVertexIndex,
            bool crossApplyForwardAdjacencyList, bool crossApplyBackwardAdjacencyList,
            bool isStartVertexTheOriginVertex,
            BooleanFunction edgePredicate, List<string> projectedFields,
            GraphViewCommand command,
            int outputRecordLength,
            int batchSize = KW_DEFAULT_BATCH_SIZE)
        {
            this.inputOp = inputOp;
            this.outputBuffer = new Queue<RawRecord>();
            this.startVertexIndex = startVertexIndex;
            this.crossApplyForwardAdjacencyList = crossApplyForwardAdjacencyList;
            this.crossApplyBackwardAdjacencyList = crossApplyBackwardAdjacencyList;
            this.isStartVertexTheOriginVertex = isStartVertexTheOriginVertex;
            this.edgePredicate = edgePredicate;
            this.projectedFields = projectedFields;
            this.command = command;

            this.batchSize = batchSize;
            this.lazyAdjacencyListBatch = new Queue<Tuple<RawRecord, string>>();

            this.outputRecordLength = outputRecordLength;

            this.inputRecordsBuffer = new Queue<RawRecord>();
            this.Open();
        }

        /// <summary>
        /// Fill edge's {_source, _sink, _other, id, *} meta fields
        /// </summary>
        /// <param name="record"></param>
        /// <param name="edge"></param>
        /// <param name="startVertexId"></param>
        /// <param name="isReversedAdjList"></param>
        /// <param name="startVertexPartition"></param>
        /// <param name="isStartVertexTheOriginVertex"></param>
        internal static void FillMetaField(RawRecord record, EdgeField edge, 
            string startVertexId, string startVertexPartition, bool isStartVertexTheOriginVertex, bool isReversedAdjList)
        {
            string otherValue, otherVPartition;
            if (isStartVertexTheOriginVertex) {
                if (isReversedAdjList) {
                    otherValue = edge.OutV;
                    otherVPartition = edge.OutVPartition;
                }
                else {
                    otherValue = edge.InV;
                    otherVPartition = edge.InVPartition;
                }
            }
            else {
                otherValue = startVertexId;
                otherVPartition = startVertexPartition;
            }

            record.fieldValues[0] = new StringField(edge.OutV);
            record.fieldValues[1] = new StringField(edge.InV);
            record.fieldValues[2] = new StringField(otherValue);
            record.fieldValues[3] = new StringField(edge.EdgeId);
            record.fieldValues[4] = new EdgeField(edge, otherValue, otherVPartition);
        }

        /// <summary>
        /// Fill the field of selected edge's properties
        /// </summary>
        /// <param name="record"></param>
        /// <param name="edge"></param>
        /// <param name="projectedFields"></param>
        internal static void FillPropertyField(RawRecord record, EdgeField edge, List<string> projectedFields)
        {
            for (int i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < projectedFields.Count; i++) {
                record.fieldValues[i] = edge[projectedFields[i]];
            }
        }

        /// <summary>
        /// Decode an adjacency list and return all the edges satisfying the edge predicate
        /// </summary>
        /// <param name="adjacencyList"></param>
        /// <param name="startVertexId"></param>
        /// <param name="isReverse"></param>
        /// <returns></returns>
        private List<RawRecord> DecodeAdjacencyList(AdjacencyListField adjacencyList, string startVertexId, string startVertexPartition, bool isReverse)
        {
            List<RawRecord> edgeRecordCollection = new List<RawRecord>();

            foreach (EdgeField edge in adjacencyList.AllEdges) {
                // Construct new record
                RawRecord edgeRecord = new RawRecord(this.projectedFields.Count);

                AdjacencyListDecoder.FillMetaField(edgeRecord, edge, startVertexId, startVertexPartition, this.isStartVertexTheOriginVertex, isReverse);
                AdjacencyListDecoder.FillPropertyField(edgeRecord, edge, this.projectedFields);

                if (this.edgePredicate != null && !this.edgePredicate.Evaluate(edgeRecord)) {
                    continue;
                }

                edgeRecordCollection.Add(edgeRecord);
            }

            return edgeRecordCollection;
        }

        /// <summary>
        /// Decode a record's adjacency list or/and reverse adjacency list
        /// and return all the edges satisfying the edge predicate
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        private List<RawRecord> Decode(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();
            VertexField startVertex = record[this.startVertexIndex] as VertexField;
            if (startVertex == null) {
                throw new GraphViewException($"{record[this.startVertexIndex].ToString()} cannot be cast to a vertex.");
            }
            string startVertexId = startVertex.VertexId;

            if (this.crossApplyForwardAdjacencyList) {
                AdjacencyListField adj = ((VertexField)record[this.startVertexIndex]).AdjacencyList;
                Debug.Assert(adj.HasBeenFetched);
                results.AddRange(this.DecodeAdjacencyList(adj, startVertexId, startVertex.Partition, false));
            }

            if (this.crossApplyBackwardAdjacencyList) {
                AdjacencyListField revAdj = ((VertexField)record[this.startVertexIndex]).RevAdjacencyList;
                Debug.Assert(revAdj.HasBeenFetched);
                results.AddRange(this.DecodeAdjacencyList(revAdj, startVertexId, startVertex.Partition, true));
            }

            return results;
        }

        /// <summary>
        /// Cross apply the adjacency list or/and reverse adjacency list of the record
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        private List<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            foreach (RawRecord edgeRecord in this.Decode(record)) {
                RawRecord r = new RawRecord(record);
                r.Append(edgeRecord);

                results.Add(r);
            }

            return results;
        }

        /// <summary>
        /// Send one query to construct all the spilled adjacency lists of vertice in the inputSequence 
        /// </summary>
        private void ConstructLazyAdjacencyListInBatch()
        {
            HashSet<string> vertexIdCollection = new HashSet<string>();
            HashSet<string> vertexPartitionKeyCollection = new HashSet<string>();
            EdgeType edgeType = 0;

            foreach (Tuple<RawRecord, string> tuple in this.lazyAdjacencyListBatch) {
                string vertexId = tuple.Item2;
                VertexField vertexField = (VertexField)(tuple.Item1[this.startVertexIndex]);

                AdjacencyListField adj = vertexField.AdjacencyList;
                AdjacencyListField revAdj = vertexField.RevAdjacencyList;
                Debug.Assert(adj != null, "adj != null");
                Debug.Assert(revAdj != null, "revAdj != null");

                bool addThisEdge = false;
                if (this.crossApplyForwardAdjacencyList && !adj.HasBeenFetched) {
                    edgeType |= EdgeType.Outgoing;
                    addThisEdge = true;
                }
                if (this.crossApplyBackwardAdjacencyList && !revAdj.HasBeenFetched) {
                    edgeType |= EdgeType.Incoming;
                    addThisEdge = true;
                }

                if (addThisEdge) {
                    vertexIdCollection.Add(vertexId);
                    if (vertexField.Partition != null) {
                        vertexPartitionKeyCollection.Add(vertexField.Partition);
                    }
                }
            }

            if (vertexIdCollection.Count > 0) {
                Debug.Assert(edgeType != 0);
                EdgeDocumentHelper.ConstructLazyAdjacencyList(this.command, edgeType, vertexIdCollection, vertexPartitionKeyCollection);
            }
        }


        public override RawRecord Next()
        {
            while (this.State())
            {
                // construct lazy list
                if (this.lazyAdjacencyListBatch.Any())
                {
                    this.ConstructLazyAdjacencyListInBatch();
                    this.lazyAdjacencyListBatch.Clear();
                }

                // cross apply and fill output buffer
                while (this.inputRecordsBuffer.Any())
                {
                    RawRecord record = this.inputRecordsBuffer.Dequeue();
                    foreach (RawRecord r in this.CrossApply(record))
                    {
                        this.outputBuffer.Enqueue(r);
                    }
                }

                // check output buffer
                if (this.outputBuffer.Any())
                {
                    return outputBuffer.Dequeue();
                }

                // get input records
                RawRecord inputRecord = null;
                this.inputRecordsBuffer.Clear();
                while (this.inputOp.State() && inputRecordsBuffer.Count < this.batchSize && (inputRecord = this.inputOp.Next()) != null)
                {
                    Debug.Assert(inputRecord.Length <= this.outputRecordLength, "inputRecord.Length <= this.outputRecordLength");

                    inputRecordsBuffer.Enqueue(inputRecord);

                    // has Been Cross Applied On Server
                    if (inputRecord.Length == this.outputRecordLength)
                    {
                        continue;
                    }

                    VertexField startVertex = inputRecord[this.startVertexIndex] as VertexField;
                    if (startVertex == null)
                    {
                        throw new GraphViewException($"{inputRecord[this.startVertexIndex]} cannot be cast to a vertex.");
                    }
                    string startVertexId = startVertex.VertexId;

                    if (this.crossApplyForwardAdjacencyList)
                    {
                        AdjacencyListField adj = startVertex.AdjacencyList;
                        Debug.Assert(adj != null, "adj != null");
                        if (!adj.HasBeenFetched)
                        {
                            this.lazyAdjacencyListBatch.Enqueue(new Tuple<RawRecord, string>(inputRecord, startVertexId));
                            continue;
                        }
                    }

                    if (this.crossApplyBackwardAdjacencyList)
                    {
                        AdjacencyListField revAdj = startVertex.RevAdjacencyList;
                        Debug.Assert(revAdj != null, "revAdj != null");
                        if (!revAdj.HasBeenFetched)
                        {
                            this.lazyAdjacencyListBatch.Enqueue(new Tuple<RawRecord, string>(inputRecord, startVertexId));
                            continue;
                        }
                    }
                }

                if (!inputRecordsBuffer.Any())
                {
                    this.Close();
                    return null;
                }
            }
            return null;
        }

        public override void ResetState()
        {
            this.inputOp.ResetState();
            this.outputBuffer.Clear();
            this.lazyAdjacencyListBatch.Clear();
            this.inputRecordsBuffer.Clear();
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.outputBuffer = new Queue<RawRecord>();
            this.lazyAdjacencyListBatch = new Queue<Tuple<RawRecord, string>>();
            this.inputRecordsBuffer = new Queue<RawRecord>();

            this.command = SerializationData.Command;
        }
    }

    [DataContract]
    internal class SubgraphOperator : GraphViewExecutionOperator
    {
        [DataMember]
        private readonly GraphViewExecutionOperator inputOp;

        private SubgraphFunction aggregateState;
        [DataMember]
        private readonly ScalarFunction getSubgraphEdgeFunction;

        private Queue<RawRecord> outputBuffer;
        [DataMember]
        private readonly string sideEffectKey;

        public SubgraphOperator(GraphViewExecutionOperator inputOp, ScalarFunction getTargetFieldFunction, 
            SubgraphFunction aggregateState, string sideEffectKey)
        {
            this.inputOp = inputOp;
            this.aggregateState = aggregateState;
            this.getSubgraphEdgeFunction = getTargetFieldFunction;
            this.outputBuffer = new Queue<RawRecord>();
            this.sideEffectKey = sideEffectKey;
            this.Open();
        }

        public override RawRecord Next()
        {
            while (this.State())
            {
                if (this.outputBuffer.Any())
                {
                    FieldObject graph = this.aggregateState.Terminate();

                    RawRecord result = this.outputBuffer.Dequeue();
                    result.Append(graph);
                    return result;
                }

                // read all input records
                RawRecord r = null;
                while (inputOp.State() && (r = inputOp.Next()) != null)
                {
                    RawRecord result = new RawRecord(r);

                    FieldObject aggregateObject = getSubgraphEdgeFunction.Evaluate(r);

                    if (aggregateObject == null)
                        throw new GraphViewException("The provided traversal or property name in Subgraph does not map to a value.");

                    aggregateState.Accumulate(aggregateObject);

                    outputBuffer.Enqueue(result);
                }

                if (!this.outputBuffer.Any())
                {
                    this.Close();
                    return null;
                }
            }
            
            return null;
        }

        public override void ResetState()
        {
            this.outputBuffer.Clear();
            this.inputOp.ResetState();
            this.Open();
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.outputBuffer = new Queue<RawRecord>();
            this.aggregateState = (SubgraphFunction)SerializationData.SideEffectStates[this.sideEffectKey];
        }
    }
}
