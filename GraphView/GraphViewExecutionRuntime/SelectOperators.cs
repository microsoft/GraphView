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
        private List<string> nodeProperties; 

        public FetchNodeOperator2(GraphViewConnection connection, JsonQuery vertexQuery, List<string> nodeProperties, int outputBufferSize = 1000)
        {
            Open();
            this.connection = connection;
            this.vertexQuery = vertexQuery;
            this.outputBufferSize = outputBufferSize;
            this.nodeProperties = nodeProperties;
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
                    //foreach (RawRecord rec in databasePortal.GetVertices(vertexQuery))
                    //{
                    //    outputBuffer.Enqueue(rec);
                    //}

                    var rawVertices = databasePortal.GetRawVertices(vertexQuery);
                    var decoder = new DocDbDecoder2();
                    foreach (var rec in decoder.GetVertices(rawVertices, nodeProperties))
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

        // The table-valued scalar function that given a record of a source vertex,
        // returns the references of the sink vertices
        private TableValuedScalarFunction crossApplySinkReference; 

        // The query that describes predicates on the sink vertices and its properties to return.
        // It is null if the sink vertex has no predicates and no properties other than sink vertex ID
        // are to be returned.  
        private JsonQuery sinkVertexQuery;

        // A list of index pairs, each specifying which field in the source record 
        // must match the field in the sink record. 
        // This list is not null when sink vertices have edges pointing back 
        // to the vertices other than the source vertices in the records by the input operator. 
        private List<Tuple<int, int>> matchingIndexes;

        // List of edges whose predicates are evaluated by server
        // and will be filtered by matchingIndexes
        private List<MatchEdge> reverseEdges;

        private List<string> nodeProperties; 

        public TraversalOperator2(
            GraphViewExecutionOperator inputOp,
            GraphViewConnection connection,
            int sinkIndex,
            JsonQuery sinkVertexQuery,
            List<Tuple<int, int>> matchingIndexes,
            List<MatchEdge> reverseEdges, 
            List<string> nodeProperties, 
            int outputBufferSize = 1000)
        {
            Open();
            this.inputOp = inputOp;
            this.connection = connection;
            this.adjacencyListSinkIndex = sinkIndex;
            this.sinkVertexQuery = sinkVertexQuery;
            this.matchingIndexes = matchingIndexes;
            this.reverseEdges = reverseEdges;
            this.nodeProperties = nodeProperties;
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
                        //foreach (RawRecord rec in databasePortal.GetVertices(toSendQuery))
                        //{
                        //    if (!sinkVertexCollection.ContainsKey(rec[0]))
                        //    {
                        //        sinkVertexCollection.Add(rec[0], new List<RawRecord>());
                        //    }
                        //    sinkVertexCollection[rec[0]].Add(rec);
                        //}

                        var rawVertices = databasePortal.GetRawVertices(toSendQuery);
                        var decoder = new DocDbDecoder2();
                        foreach (var rec in decoder.GetVertices(rawVertices, nodeProperties, reverseEdges))
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
    }

    internal class AdjacencyListDecoder : TableValuedFunction
    {
        private GraphViewExecutionOperator input;
        private int adjacencyListIndex;
        private BooleanFunction edgePredicate;
        private List<string> projectedFields;
        private string edgeTableAlias;
        private int outputBufferSize;
        private Queue<RawRecord> outputBuffer;

        public AdjacencyListDecoder(GraphViewExecutionOperator input, int adjacencyListIndex,
            BooleanFunction edgePredicate, List<string> projectedFields, string edgeTableAlias, int outputBufferSize = 1000)
            : base(input, 0, 1000, 1000)
        {
            this.input = input;
            this.adjacencyListIndex = adjacencyListIndex;
            this.edgePredicate = edgePredicate;
            this.projectedFields = projectedFields;
            this.edgeTableAlias = edgeTableAlias;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            string jsonArray = record[adjacencyListIndex];
            List<RawRecord> results = new List<RawRecord>();
            // The first column is for '_sink' by default
            projectedFields.Insert(0, "_sink");

            // Parse the adj list in JSON array
            var adj = JArray.Parse(jsonArray);
            foreach (var edge in adj.Children<JObject>())
            {
                // Construct new record
                var result = new RawRecord(projectedFields.Count);

                // Fill the field of selected edge's properties
                // TODO: support wildcard *
                for (var i = 0; i < projectedFields.Count; i++)
                {
                    var fieldValue = edge[projectedFields[i]];
                    if (fieldValue != null)
                        result.fieldValues[i] = fieldValue.ToString();
                }

                results.Add(result);
            }

            return results;
        }

        public override RawRecord Next()
        {
            if (outputBuffer == null)
                outputBuffer = new Queue<RawRecord>();

            while (outputBuffer.Count < outputBufferSize && input.State())
            {
                RawRecord record = input.Next();
                if (record == null)
                    continue;
                var results = CrossApply(record);
                foreach (var edgeRecord in results)
                {
                    if (!edgePredicate.Evaluate(edgeRecord))
                        continue;

                    record.Append(edgeRecord);
                    outputBuffer.Enqueue(record);
                }
            }

            if (outputBuffer.Count == 0)
            {
                if (!input.State())
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
            if (traversalOutputBuffer.Count > 0)
            {
                RawRecord r = new RawRecord(currentRecord);
                RawRecord traversalRec = traversalOutputBuffer.Dequeue();
                r.Append(traversalRec);

                return r;
            }

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
    }
}
