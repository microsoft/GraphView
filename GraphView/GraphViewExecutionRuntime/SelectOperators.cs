using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    /// traverses to their one-hop or multi-hop neighbors. One-hop vertices
    /// are defined in the adjacency list of the sources. Multi-hop
    /// vertices are usually defined as a (recursive) function that takes a vertex as input
    /// and produces one or more vertex references. 
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
        private int adjacencyListIndex = -1;

        private TableValuedScalarFunction crossApplySinkReference; 

        // The query that describes predicates on the sink vertices and the properties to return.
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
            int adjacencyListIndex,
            JsonQuery sinkVertexQuery,
            List<Tuple<int, int>> matchingIndexes,
            GraphViewConnection connection,
            int outputBufferSize = 1000)
        {
            Open();
            this.inputOp = inputOp;
            this.adjacencyListIndex = adjacencyListIndex;
            this.sinkVertexQuery = sinkVertexQuery;
            this.matchingIndexes = matchingIndexes;
            this.connection = connection;
            this.outputBufferSize = outputBufferSize;

            crossApplySinkReference = new CrossApplyAdjacencyList(adjacencyListIndex);
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

                // Loads X input records and populates their sink references 
                for (int i = 0; i < batchSize && inputOp.State(); i++)
                {
                    RawRecord record = inputOp.Next();
                    if (record == null)
                    {
                        break;
                    }

                    foreach (string sinkId in crossApplySinkReference.Apply(record))
                    {
                        inputSequence.Add(new Tuple<RawRecord, string>(record, sinkId));
                    }
                }

                // Groups results by sink vertices' references
                Dictionary<string, List<RawRecord>> sinkVertexCollection = new Dictionary<string, List<RawRecord>>(inClauseLimit);

                int j = 0;
                HashSet<string> sinkReferenceSet = new HashSet<string>();
                StringBuilder sinkReferenceList = new StringBuilder();
                while (j < inputSequence.Count)
                {
                    sinkReferenceSet.Clear();

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
}
