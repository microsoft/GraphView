using System;
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
        private string vertexQuery;
        private GraphViewConnection connection;

        public FetchNodeOperator2(GraphViewConnection connection, string vertexQuery, int outputBufferSize = 1000)
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
                if (!State())
                {
                    return null;
                }

                // If the output buffer is empty and this operator is still active, 
                // sends a query to the underlying system retrieving up to X results, 
                // where X is the capacity of the output buffer.
                using (DbPortal databasePortal = connection.CreateDatabasePortal())
                {
                    int count = 0;
                    foreach (RawRecord rec in databasePortal.GetVertices(vertexQuery))
                    {
                        outputBuffer.Enqueue(rec);
                        count++;

                        if (count >= outputBufferSize)
                        {
                            break;
                        }
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
    /// are defined in the adjacency list of the sources. And multi-hop
    /// vertices are usually defined as a (recursive) function that takes a vertex as input
    /// and produces one or more vertex references. 
    /// 
    /// This operators emulates the nested-loop join algorithm.
    /// </summary>
    internal class TraversalOperator2 : GraphViewExecutionOperator
    {
        private int outputBufferSize;
        private Queue<RawRecord> outputBuffer;
        private GraphViewConnection connection;
        private GraphViewExecutionOperator inputOp;
        
        // The index of the adjacency list in the record from which the traversal starts
        private int adjacencyListIndex;

        // The query that describes predicates on the sink vertices and the properties to return.
        // It is null if the sink vertex has no predicates and no properties other than sink vertex ID
        // are to be returned.  
        private string sinkVertexQuery;

        // A list of index pairs, each specifying which field in the source record 
        // must match the field in the sink record. 
        // This list is not null when sink vertices have edges pointing back 
        // to the vertices other than the source vertices in the records by the input operator. 
        private List<Tuple<int, int>> matchingIndexes;

        public override RawRecord Next()
        {
            throw new NotImplementedException();
        }
    }
}
