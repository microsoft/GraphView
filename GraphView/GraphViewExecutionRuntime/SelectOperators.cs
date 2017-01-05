using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GraphViewExecutionRuntime;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    internal class FetchNodeOperator2 : GraphViewExecutionOperator
    {
        private Queue<RawRecord> outputBuffer;
        private int outputBufferSize;
        private JsonQuery vertexQuery;
        private GraphViewConnection connection;

        public FetchNodeOperator2(GraphViewConnection connection, JsonQuery vertexQuery, int outputBufferSize = 1000)
        {
            Open();
            this.connection = connection;
            this.vertexQuery = vertexQuery;
            this.outputBufferSize = outputBufferSize;
        }

        public override RawRecord Next()
        {
            if (outputBuffer == null)
                outputBuffer = new Queue<RawRecord>(outputBufferSize);

            if (outputBuffer.Count == 0)
            {
                // If the output buffer is empty, sends a query to the underlying system 
                // retrieving all the vertices satisfying the query.
                using (DbPortal databasePortal = connection.CreateDatabasePortal())
                {
                    foreach (RawRecord rec in databasePortal.GetVertices(vertexQuery))
                    {
                        outputBuffer.Enqueue(rec);
                    }
                }
            }

            if (outputBuffer.Count == 0)
            {
                return null;
            }
            else if (outputBuffer.Count == 1)
            {
                Close();
                return outputBuffer.Dequeue();
            }
            else
            {
                return outputBuffer.Dequeue();
            }
        }

        public override void ResetState()
        {
            Open();
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
    internal class TraversalOperator2 : GraphViewExecutionOperator
    {
        private int outputBufferSize;
        private int batchSize = 100;
        private int inClauseLimit = 200;
        private Queue<RawRecord> outputBuffer;
        private GraphViewConnection connection;
        private GraphViewExecutionOperator inputOp;
        
        // The index of the adjacency list in the record from which the traversal starts
        private int adjacencyListSinkIndex = -1;

        // The query that describes predicates on the sink vertices and its properties to return.
        // It is null if the sink vertex has no predicates and no properties other than sink vertex ID
        // are to be returned.  
        private JsonQuery sinkVertexQuery;

        // A list of index pairs, each specifying which field in the source record 
        // must match the field in the sink record. 
        // This list is not null when sink vertices have edges pointing back 
        // to the vertices other than the source vertices in the records by the input operator. 
        private List<Tuple<int, int>> matchingIndexes;

        public TraversalOperator2(
            GraphViewExecutionOperator inputOp,
            GraphViewConnection connection,
            int sinkIndex,
            JsonQuery sinkVertexQuery,
            List<Tuple<int, int>> matchingIndexes,
            int outputBufferSize = 1000)
        {
            Open();
            this.inputOp = inputOp;
            this.connection = connection;
            this.adjacencyListSinkIndex = sinkIndex;
            this.sinkVertexQuery = sinkVertexQuery;
            this.matchingIndexes = matchingIndexes;
            this.outputBufferSize = outputBufferSize;
        }

        public override RawRecord Next()
        {
            if (outputBuffer == null)
            {
                outputBuffer = new Queue<RawRecord>(outputBufferSize);
            }

            while (outputBuffer.Count < outputBufferSize && inputOp.State())
            {
                List<Tuple<RawRecord, string>> inputSequence = new List<Tuple<RawRecord, string>>(batchSize);

                // Loads a batch of source records
                for (int i = 0; i < batchSize && inputOp.State(); i++)
                {
                    RawRecord record = inputOp.Next();
                    if (record == null)
                    {
                        break;
                    }

                    inputSequence.Add(new Tuple<RawRecord, string>(record, record[adjacencyListSinkIndex]));
                }

                // TODO: Figure out whether this condition will have an influence on RawRecordLayout 
                // When sinkVertexQuery is null, only sink vertices' IDs are to be returned. 
                // As a result, there is no need to send queries the underlying system to retrieve 
                // the sink vertices.  
                if (sinkVertexQuery == null)
                {
                    foreach (Tuple<RawRecord, string> pair in inputSequence)
                    {
                        RawRecord resultRecord = new RawRecord(pair.Item1.Length);
                        resultRecord.Append(pair.Item1);
                        resultRecord.Append(pair.Item2);
                        outputBuffer.Enqueue(resultRecord);
                    }

                    continue;
                }

                // Groups records returned by sinkVertexQuery by sink vertices' references
                Dictionary<string, List<RawRecord>> sinkVertexCollection = new Dictionary<string, List<RawRecord>>(inClauseLimit);

                HashSet<string> sinkReferenceSet = new HashSet<string>();
                StringBuilder sinkReferenceList = new StringBuilder();
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

                    //TODO: Verify whether DocumentDB still has inClauseLimit
                    while (sinkReferenceSet.Count < inClauseLimit && j < inputSequence.Count)
                    {
                        sinkReferenceSet.Add(inputSequence[j].Item2);
                        j++;
                    }

                    sinkReferenceList.Clear();
                    foreach (string sinkRef in sinkReferenceSet)
                    {
                        if (sinkReferenceList.Length > 0)
                        {
                            sinkReferenceList.Append(", ");
                        }
                        sinkReferenceList.Append(sinkRef);
                    }

                    string inClause = string.Format("{0}.id IN ({1})", sinkVertexQuery.Alias, sinkReferenceList.ToString());

                    JsonQuery toSendQuery = new JsonQuery()
                    {
                        Alias = sinkVertexQuery.Alias,
                        WhereSearchCondition = sinkVertexQuery.WhereSearchCondition,
                        SelectClause = sinkVertexQuery.SelectClause,
                        ProjectedColumns = sinkVertexQuery.ProjectedColumns
                    };
                    if (toSendQuery.WhereSearchCondition == null)
                    {
                        toSendQuery.WhereSearchCondition = inClause;
                    }
                    else
                    {
                        toSendQuery.WhereSearchCondition = 
                            string.Format("({0}) AND {1}", sinkVertexQuery.WhereSearchCondition, inClause);
                    }

                    using (DbPortal databasePortal = connection.CreateDatabasePortal())
                    {
                        foreach (RawRecord rec in databasePortal.GetVertices(toSendQuery))
                        {
                            if (!sinkVertexCollection.ContainsKey(rec[0]))
                            {
                                sinkVertexCollection.Add(rec[0], new List<RawRecord>());
                            }
                            sinkVertexCollection[rec[0]].Add(rec);
                        }
                    }
                }

                foreach (Tuple<RawRecord, string> pair in inputSequence)
                {
                    if (!sinkVertexCollection.ContainsKey(pair.Item2))
                    {
                        continue;
                    }

                    RawRecord sourceRec = pair.Item1;
                    List<RawRecord> sinkRecList = sinkVertexCollection[pair.Item2];
                    
                    foreach (RawRecord sinkRec in sinkRecList)
                    {
                        if (matchingIndexes != null && matchingIndexes.Count > 0)
                        {
                            int k = 0;
                            for (; k < matchingIndexes.Count; k++)
                            {
                                int sourceMatchIndex = matchingIndexes[k].Item1;
                                int sinkMatchIndex = matchingIndexes[k].Item2;
                                if (sourceRec[sourceMatchIndex] != sinkRec[sinkMatchIndex])
                                {
                                    break;
                                }
                            }

                            // The source-sink record pair is the result only when it passes all matching tests. 
                            if (k < matchingIndexes.Count)
                            {
                                continue;
                            }
                        }

                        RawRecord resultRec = new RawRecord(sourceRec);
                        resultRec.Append(sinkRec);

                        outputBuffer.Enqueue(resultRec);
                    }
                }
            }

            if (outputBuffer.Count == 0)
            {
                if (!inputOp.State())
                    Close();
                return null;
            }
            else if (outputBuffer.Count == 1)
            {
                Close();
                return outputBuffer.Dequeue();
            }
            else
            {
                return outputBuffer.Dequeue();
            }
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }
    }

    internal class CartesianProductOperator2 : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator leftInput;
        private ContainerEnumerator rightInputEnumerator;
        private RawRecord leftRecord;

        public CartesianProductOperator2(
            GraphViewExecutionOperator leftInput, 
            GraphViewExecutionOperator rightInput)
        {
            this.leftInput = leftInput;
            ContainerOperator rightInputContainer = new ContainerOperator(rightInput);
            rightInputEnumerator = rightInputContainer.GetEnumerator();
            leftRecord = null;
            Open();
        }

        public override RawRecord Next()
        {
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
                    if (rightInputEnumerator.MoveNext())
                    {
                        RawRecord rightRecord = rightInputEnumerator.Current;
                        cartesianRecord = new RawRecord(leftRecord);
                        cartesianRecord.Append(rightRecord);
                    }
                    else
                    {
                        // For the current left record, the enumerator on the right input has reached the end.
                        // Moves to the next left record and resets the enumerator.
                        rightInputEnumerator.Reset();
                        leftRecord = null;
                    }
                }
            }

            return cartesianRecord;
        }

        public override void ResetState()
        {
            leftInput.ResetState();
            Open();
        }
    }

    internal class AdjacencyListDecoder : TableValuedFunction
    {
        private List<int> adjacencyListIndexes;
        private BooleanFunction edgePredicate;
        private List<string> projectedFields;

        public AdjacencyListDecoder(GraphViewExecutionOperator input, List<int> adjacencyListIndexes,
            BooleanFunction edgePredicate, List<string> projectedFields, int outputBufferSize = 1000)
            : base(input, outputBufferSize)
        {
            this.adjacencyListIndexes = adjacencyListIndexes;
            this.edgePredicate = edgePredicate;
            this.projectedFields = projectedFields;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            List<RawRecord> results = new List<RawRecord>();

            foreach (var adjIndex in adjacencyListIndexes)
            {
                string jsonArray = record[adjIndex];
                // Parse the adj list in JSON array
                var adj = JArray.Parse(jsonArray);
                foreach (var edge in adj.Children<JObject>())
                {
                    // Construct new record
                    var result = new RawRecord(projectedFields.Count);

                    // Fill the field of selected edge's properties
                    for (var i = 0; i < projectedFields.Count; i++)
                    {
                        var projectedField = projectedFields[i];
                        var fieldValue = "*".Equals(projectedField, StringComparison.OrdinalIgnoreCase)
                            ? edge
                            : edge[projectedField];
                        if (fieldValue != null)
                            result.fieldValues[i] = fieldValue.ToString();
                    }

                    results.Add(result);
                }
            }

            return results;
        }

        public override RawRecord Next()
        {
            if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();

            while (OutputBuffer.Count < OutputBufferSize && InputOperator.State())
            {
                RawRecord srcRecord = InputOperator.Next();
                if (srcRecord == null)
                    break;

                var results = CrossApply(srcRecord);
                foreach (var edgeRecord in results)
                {
                    if (!edgePredicate.Evaluate(edgeRecord))
                        continue;

                    var resultRecord = new RawRecord(srcRecord);
                    resultRecord.Append(edgeRecord);
                    OutputBuffer.Enqueue(resultRecord);
                }
            }

            if (OutputBuffer.Count == 0)
            {
                if (!InputOperator.State())
                    Close();
                return null;
            }
            else if (OutputBuffer.Count == 1)
            {
                Close();
                return OutputBuffer.Dequeue();
            }
            else
            {
                return OutputBuffer.Dequeue();
            }
        }

        public override void ResetState()
        {
            InputOperator.ResetState();
            Open();
        }
    }

    internal abstract class TableValuedScalarFunction
    {
        public abstract IEnumerable<string> Apply(RawRecord record);
    }

    internal class CrossApplyAdjacencyList : TableValuedScalarFunction
    {
        private int adjacencyListIndex;

        public CrossApplyAdjacencyList(int adjacencyListIndex)
        {
            this.adjacencyListIndex = adjacencyListIndex;
        }

        public override IEnumerable<string> Apply(RawRecord record)
        {
            throw new NotImplementedException();
        }
    }

    internal class CrossApplyPath : TableValuedScalarFunction
    {
        private GraphViewExecutionOperator referenceOp;
        private ConstantSourceOperator contextScan;
        private ExistsFunction terminateFunction;
        private int iterationUpperBound;

        public CrossApplyPath(
            ConstantSourceOperator contextScan, 
            GraphViewExecutionOperator referenceOp,
            int iterationUpperBound)
        {
            this.contextScan = contextScan;
            this.referenceOp = referenceOp;
            this.iterationUpperBound = iterationUpperBound;
        }

        public CrossApplyPath(
            ConstantSourceOperator contextScan,
            GraphViewExecutionOperator referenceOp,
            ExistsFunction terminateFunction)
        {
            this.contextScan = contextScan;
            this.referenceOp = referenceOp;
            this.terminateFunction = terminateFunction;
        }

        public override IEnumerable<string> Apply(RawRecord record)
        {
            contextScan.ConstantSource = record;

            if (terminateFunction != null)
            {
                throw new NotImplementedException();
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }

    internal interface IAggregateFunction
    {
        void Init();
        void Accumulate(params string[] values);

        string Terminate();
    }

    internal class ProjectOperator : GraphViewExecutionOperator
    {
        private List<Tuple<ScalarFunction, string>> selectScalarList;
        private GraphViewExecutionOperator inputOp;

        private RawRecord currentRecord;
        private Queue<RawRecord> outputBuffer;

        public ProjectOperator(GraphViewExecutionOperator inputOp)
        {
            this.inputOp = inputOp;
            selectScalarList = new List<Tuple<ScalarFunction, string>>();
            outputBuffer = new Queue<RawRecord>();
        }

        public void AddSelectScalarElement(ScalarFunction scalarFunction, string alias)
        {
            selectScalarList.Add(new Tuple<ScalarFunction, string>(scalarFunction, alias));
        }

        public override RawRecord Next()
        {
            currentRecord = inputOp.Next();
            if (currentRecord == null)
            {
                Close();
                return null;
            }

            RawRecord selectRecord = new RawRecord(selectScalarList.Count);
            int index = 0;
            foreach (var selectPair in selectScalarList)
            {
                ScalarFunction scalarFunction = selectPair.Item1;
                string result = scalarFunction.Evaluate(currentRecord);
                selectRecord.fieldValues[index++] = result ?? "";
            }

            return selectRecord;
        }
    }

    internal class ProjectAggregation : GraphViewExecutionOperator
    {
        List<Tuple<IAggregateFunction, List<int>>> aggregationSpecs;
        GraphViewExecutionOperator inputOp;

        public ProjectAggregation(GraphViewExecutionOperator inputOp)
        {
            this.inputOp = inputOp;
            aggregationSpecs = new List<Tuple<IAggregateFunction, List<int>>>();
        }

        public void AddAggregateSpec(IAggregateFunction aggrFunc, List<int> aggrInputIndexes)
        {
            aggregationSpecs.Add(new Tuple<IAggregateFunction, List<int>>(aggrFunc, aggrInputIndexes));
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }

        public override RawRecord Next()
        {
            foreach (var aggr in aggregationSpecs)
            {
                aggr.Item1.Init();
            }

            RawRecord inputRec = null;
            while ((inputRec = inputOp.Next()) != null)
            {
                foreach (var aggr in aggregationSpecs)
                {
                    string[] paraList = new string[aggr.Item2.Count];
                    for(int i = 0; i < aggr.Item2.Count; i++)
                    {
                        paraList[i] = inputRec[aggr.Item2[i]];
                    }

                    aggr.Item1.Accumulate(paraList);
                }
            }

            RawRecord outputRec = new RawRecord();
            foreach (var aggr in aggregationSpecs)
            {
                outputRec.Append(aggr.Item1.Terminate());
            }

            return outputRec;
        }
    }

    internal class FlatMapOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the flatMap function.
        private GraphViewExecutionOperator flatMapTraversal;
        private ConstantSourceOperator contextOp;

        private RawRecord currentRecord = null;
        private Queue<RawRecord> outputBuffer;

        public FlatMapOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator flatMapTraversal,
            ConstantSourceOperator contextOp)
        {
            this.inputOp = inputOp;
            this.flatMapTraversal = flatMapTraversal;
            this.contextOp = contextOp;

            outputBuffer = new Queue<RawRecord>();
        }

        public override RawRecord Next()
        {
            if (outputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord toAppend = outputBuffer.Dequeue();
                r.Append(toAppend);

                return r;
            }

            currentRecord = inputOp.Next();
            if (currentRecord == null)
            {
                Close();
                return null;
            }

            contextOp.ConstantSource = currentRecord;
            flatMapTraversal.ResetState();
            RawRecord localRec = null;
            while ((localRec = flatMapTraversal.Next()) != null)
            {
                outputBuffer.Enqueue(localRec);
            }

            if (outputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord toAppend = outputBuffer.Dequeue();
                r.Append(toAppend);

                return r;
            }
            else
            {
                Close();
                return null;
            }
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }
    }

    internal class LocalOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;

        // The traversal inside the local function.
        private GraphViewExecutionOperator localTraversal;
        private ConstantSourceOperator contextOp;

        private RawRecord currentRecord = null;
        private Queue<RawRecord> outputBuffer;

        public LocalOperator(
            GraphViewExecutionOperator inputOp,
            GraphViewExecutionOperator localTraversal,
            ConstantSourceOperator contextOp)
        {
            this.inputOp = inputOp;
            this.localTraversal = localTraversal;
            this.contextOp = contextOp;

            outputBuffer = new Queue<RawRecord>();
        }

        public override RawRecord Next()
        {
            if (outputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord toAppend = outputBuffer.Dequeue();
                r.Append(toAppend);

                return r;
            }

            currentRecord = inputOp.Next();
            if (currentRecord == null)
            {
                Close();
                return null;
            }

            contextOp.ConstantSource = currentRecord;
            localTraversal.ResetState();
            RawRecord localRec = null;
            while ((localRec = localTraversal.Next()) != null)
            {
                outputBuffer.Enqueue(localRec);
            }

            if (outputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord toAppend = outputBuffer.Dequeue();
                r.Append(toAppend);

                return r;
            }
            else
            {
                Close();
                return null;
            }
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }
    }

    internal class OptionalOperator : GraphViewExecutionOperator
    {
        private GraphViewExecutionOperator inputOp;
        // A list of record fields (identified by field indexes) from the input 
        // operator are to be returned when the optional traversal produces no results.
        // When a field index is less than 0, it means that this field value is always null. 
        private List<int> inputIndexes;

        // The traversal inside the optional function. 
        // The records returned by this operator should have the same number of fields
        // as the records drawn from the input operator, i.e., inputIndexes.Count 
        private GraphViewExecutionOperator optionalTraversal;
        private ConstantSourceOperator contextOp;

        RawRecord currentRecord = null;
        private Queue<RawRecord> outputBuffer;

        public OptionalOperator(
            GraphViewExecutionOperator inputOp,
            List<int> inputIndexes,
            GraphViewExecutionOperator optionalTraversal, 
            ConstantSourceOperator contextOp)
        {
            this.inputOp = inputOp;
            this.inputIndexes = inputIndexes;
            this.optionalTraversal = optionalTraversal;
            this.contextOp = contextOp;

            outputBuffer = new Queue<RawRecord>();
        }

        public override RawRecord Next()
        {
            if (outputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord toAppend = outputBuffer.Dequeue();
                r.Append(toAppend);

                return r;
            }

            currentRecord = inputOp.Next();
            if (currentRecord == null)
            {
                Close();
                return null;
            }

            contextOp.ConstantSource = currentRecord;
            optionalTraversal.ResetState();
            RawRecord optionalRec = null;
            while ((optionalRec = optionalTraversal.Next()) != null)
            {
                outputBuffer.Enqueue(optionalRec);
            }

            if (outputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord toAppend = outputBuffer.Dequeue();
                r.Append(toAppend);

                return r;
            }
            else
            {
                RawRecord r = new RawRecord(currentRecord);
                foreach (int index in inputIndexes)
                {
                    if (index < 0)
                    {
                        r.Append((string)null);
                    }
                    else
                    {
                        r.Append(currentRecord[index]);
                    }
                }

                return r;
            }
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }
    }

    internal class CoalesceOperator2 : GraphViewExecutionOperator
    {
        private List<Tuple<ConstantSourceOperator, GraphViewExecutionOperator>> traversalList;
        private GraphViewExecutionOperator inputOp;

        private RawRecord currentRecord;
        private Queue<RawRecord> traversalOutputBuffer;

        public CoalesceOperator2(GraphViewExecutionOperator inputOp)
        {
            this.inputOp = inputOp;
            traversalList = new List<Tuple<ConstantSourceOperator, GraphViewExecutionOperator>>();
            traversalOutputBuffer = new Queue<RawRecord>();
        }

        public void AddTraversal(ConstantSourceOperator contextOp, GraphViewExecutionOperator traversal)
        {
            traversalList.Add(new Tuple<ConstantSourceOperator, GraphViewExecutionOperator>(contextOp, traversal));
        }

        public override RawRecord Next()
        {
            while (traversalOutputBuffer.Count == 0 && inputOp.State())
            {
                currentRecord = inputOp.Next();
                if (currentRecord == null)
                {
                    Close();
                    return null;
                }

                foreach (var traversalPair in traversalList)
                {
                    ConstantSourceOperator traversalContext = traversalPair.Item1;
                    GraphViewExecutionOperator traversal = traversalPair.Item2;
                    traversalContext.ConstantSource = currentRecord;
                    traversal.ResetState();

                    RawRecord traversalRec = null;
                    while ((traversalRec = traversal.Next()) != null)
                    {
                        traversalOutputBuffer.Enqueue(traversalRec);
                    }

                    if (traversalOutputBuffer.Count > 0)
                    {
                        break;
                    }
                }
            }

            if (traversalOutputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord traversalRec = traversalOutputBuffer.Dequeue();
                r.Append(traversalRec);

                return r;
            }
            else
            {
                Close();
                return null;
            }
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }
    }

    internal class RepeatOperator : GraphViewExecutionOperator
    {
        // Number of times the inner operator repeats itself.
        // If this number is less than 0, the termination condition 
        // is specified by a boolean function. 
        private int repeatTimes;

        // The termination condition of iterations
        private BooleanFunction terminationCondition;
        // If this variable is true, the iteration starts with the context record. 
        // This corresponds to the while-do loop semantics. 
        // Otherwise, the iteration starts with the the output of the first execution of the inner operator,
        // which corresponds to the do-while loop semantics.
        private bool startFromContext;
        // The condition determining whether or not an intermediate state is emitted
        private BooleanFunction emitCondition;
        // This variable specifies whether or not the context record is considered 
        // to be emitted when the iteration does not start with the context record,
        // i.e., startFromContext is false 
        private bool emitContext;

        private GraphViewExecutionOperator inputOp;
        // A list record fields (identified by field indexes) from the input 
        // operator that are fed as the initial input into the inner operator.
        private List<int> inputFieldIndexes;

        private GraphViewExecutionOperator innerOp;
        private ConstantSourceOperator innerContextOp;

        Queue<RawRecord> repeatResultBuffer;
        RawRecord currentRecord;

        public RepeatOperator(
            GraphViewExecutionOperator inputOp,
            List<int> inputFieldIndexes,
            GraphViewExecutionOperator innerOp,
            ConstantSourceOperator innerContextOp,
            int repeatTimes,
            BooleanFunction emitCondition,
            bool emitContext)
        {
            this.inputOp = inputOp;
            this.inputFieldIndexes = inputFieldIndexes;
            this.innerOp = innerOp;
            this.innerContextOp = innerContextOp;
            this.repeatTimes = repeatTimes;
            this.emitCondition = emitCondition;
            this.emitContext = emitContext;

            startFromContext = false;

            repeatResultBuffer = new Queue<RawRecord>();
        }

        public RepeatOperator(
            GraphViewExecutionOperator inputOp,
            List<int> inputFieldIndexes,
            GraphViewExecutionOperator innerOp,
            ConstantSourceOperator innerContextOp,
            BooleanFunction terminationCondition,
            bool startFromContext,
            BooleanFunction emitCondition,
            bool emitContext)
        {
            this.inputOp = inputOp;
            this.inputFieldIndexes = inputFieldIndexes;
            this.innerOp = innerOp;
            this.innerContextOp = innerContextOp;
            this.terminationCondition = terminationCondition;
            this.startFromContext = startFromContext;
            this.emitCondition = emitCondition;
            this.emitContext = emitContext;

            repeatResultBuffer = new Queue<RawRecord>();
        }

        public override RawRecord Next()
        {
            while (repeatResultBuffer.Count == 0 && inputOp.State())
            {
                currentRecord = inputOp.Next();
                if (currentRecord == null)
                {
                    Close();
                    return null;
                }

                RawRecord initialRec = new RawRecord();
                foreach (int fieldIndex in inputFieldIndexes)
                {
                    initialRec.Append(currentRecord[fieldIndex]);
                }

                if (repeatTimes >= 0)
                {
                    // By current implementation of Gremlin, when repeat time is set to 0,
                    // it is reset to 1.
                    repeatTimes = repeatTimes == 0 ? 1 : repeatTimes;

                    Queue<RawRecord> priorStates = new Queue<RawRecord>();
                    Queue<RawRecord> newStates = new Queue<RawRecord>();

                    if (emitCondition != null && emitContext)
                    {
                        if (emitCondition.Evaluate(initialRec))
                        {
                            repeatResultBuffer.Enqueue(initialRec);
                        }
                    }

                    // Evaluates the loop for the first time
                    innerContextOp.ConstantSource = initialRec;
                    innerOp.ResetState();
                    RawRecord newRec = null;
                    while ((newRec = innerOp.Next()) != null)
                    {
                        priorStates.Enqueue(newRec);
                    }

                    // Evaluates the remaining number of iterations
                    for (int i = 0; i < repeatTimes - 1; i++)
                    {
                        while (priorStates.Count > 0)
                        {
                            RawRecord priorRec = priorStates.Dequeue();
                            innerContextOp.ConstantSource = priorRec;
                            innerOp.ResetState();
                            newRec = null;
                            while ((newRec = innerOp.Next()) != null)
                            {
                                newStates.Enqueue(newRec);

                                if (emitCondition != null && emitCondition.Evaluate(newRec))
                                {
                                    repeatResultBuffer.Enqueue(newRec);
                                }
                            }
                        }

                        var tmpQueue = priorStates;
                        priorStates = newStates;
                        newStates = tmpQueue;
                    }

                    foreach (RawRecord resultRec in newStates)
                    {
                        repeatResultBuffer.Enqueue(resultRec);
                    }
                }
                else 
                {
                    Queue<RawRecord> states = new Queue<RawRecord>();

                    if (startFromContext)
                    {
                        if (terminationCondition.Evaluate(initialRec))
                        {
                            repeatResultBuffer.Enqueue(initialRec);
                        }
                        else if (emitContext)
                        {
                            if (emitCondition == null || emitCondition.Evaluate(initialRec))
                            {
                                repeatResultBuffer.Enqueue(initialRec);
                            }
                        }
                    }
                    else
                    {
                        if (emitContext && emitCondition != null)
                        {
                            if (emitCondition.Evaluate(initialRec))
                            {
                                repeatResultBuffer.Enqueue(initialRec);
                            }
                        }
                    }

                    // Evaluates the loop for the first time
                    innerContextOp.ConstantSource = initialRec;
                    innerOp.ResetState();
                    RawRecord newRec = null;
                    while ((newRec = innerOp.Next()) != null)
                    {
                        states.Enqueue(newRec);
                    }

                    // Evaluates the remaining iterations
                    while (states.Count > 0)
                    {
                        RawRecord stateRec = states.Dequeue();

                        if (terminationCondition.Evaluate(stateRec))
                        {
                            repeatResultBuffer.Enqueue(stateRec);
                        }
                        else
                        {
                            if (emitCondition != null && emitCondition.Evaluate(stateRec))
                            {
                                repeatResultBuffer.Enqueue(stateRec);
                            }

                            innerContextOp.ConstantSource = stateRec;
                            innerOp.ResetState();
                            RawRecord loopRec = null;
                            while ((loopRec = innerOp.Next()) != null)
                            {
                                states.Enqueue(loopRec);
                            }
                        }
                    }
                }
            }

            if (repeatResultBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord repeatRecord = repeatResultBuffer.Dequeue();
                r.Append(repeatRecord);

                return r;
            }
            else
            {
                Close();
                return null;
            }
        }

        public override void ResetState()
        {
            inputOp.ResetState();
            Open();
        }
    }
}
