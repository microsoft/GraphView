using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Text;
using GraphView.GraphViewExecutionRuntime;
// Add DocumentDB references
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{

    public class JTokenComparer : IEqualityComparer<JToken>
    {
        public bool Equals(JToken x, JToken y)
        {
            if (Object.ReferenceEquals(x, y)) return true;
            if (Object.ReferenceEquals(x, null) || Object.ReferenceEquals(y, null)) return false;
            return x["_ID"].ToString().Equals(y["_ID"].ToString());
        }

        public int GetHashCode(JToken obj)
        {
            if (Object.ReferenceEquals(obj, null)) return 0;
            return obj["_ID"].ToString().GetHashCode();
        }
    }


    /// <summary>
    /// Base class of the traversal operator and the fetch-node operator.
    /// providing common properties and utility functions. 
    /// </summary>
    internal abstract class TraversalBaseOperator : GraphViewExecutionOperator
    {
        // Predicates of cross-document joins, excluding joins between edge references and vertex ID's. 
        internal BooleanFunction crossDocumentJoinPredicates;

        internal GraphViewConnection connection;

        // The meta header length of the current node
        internal int metaHeaderLength;

        // For the ith node processed, Record[3*i + 1] is its adjacent list, Record[3*i + 2] is its reverse adjacent list.
        // The last element of it is the path (Record[count - 1]).
        internal const int ADJ_OFFSET = 1;
        internal const int REV_ADJ_OFFSET = 2;

        internal const int PATH_OFFSET = -1;

        /// <summary>
        /// Removes the last k symbols from the end of a string
        /// </summary>
        /// <param name="script">The string to be trimmed</param>
        /// <param name="len">Number of symbols to be removed</param>
        /// <returns>The trimmed string</returns>
        internal static string CutTheTail(string script, int len = 1)
        {
            if (script.Length == 0) return "";
            return script.Substring(0, script.Length - len);
        }

        // Check whether the giving record r satisfy the giving boolean check function. 
        internal static bool RecordFilter(BooleanFunction BooleanCheck, RawRecord r)
        {
            if (BooleanCheck == null) return true;
            else return BooleanCheck.Evaluate(r);
        }

        /// <summary>
        /// Constructs a new record produced by this operator
        /// </summary>
        /// <param name="itemInfo">Info of the node newly processed by this operator</param>
        /// <param name="oldRecord">The input record of the operator</param>
        /// <param name="header"></param>
        /// <param name="nodeIdx">The current node id's index</param>
        /// <param name="metaHeaderLength">The current node's meta header length</param>
        /// <param name="appendedPath">Whether to produce a path string for all the nodes processed so far</param>
        /// <returns>The record to be returned</returns>
        internal static RawRecord ConstructRawRecord(Tuple<string, Dictionary<string, string>, List<string>> itemInfo, RawRecord oldRecord, List<string> header, int nodeIdx, int metaHeaderLength, string appendedPath = "")
        {
            // copy the old record's field values into new record
            RawRecord NewRecord = new RawRecord(oldRecord);

            // put current node's meta info into new record.
            NewRecord.fieldValues[nodeIdx] = itemInfo.Item1;
            for (var i = nodeIdx + 1; i < nodeIdx + metaHeaderLength - 1; i += 2)
                NewRecord.fieldValues[i] = itemInfo.Item2[header[i]];

            // extend the path if needed.
            if (appendedPath != "") NewRecord.fieldValues[NewRecord.fieldValues.Count + PATH_OFFSET] = appendedPath;
            NewRecord.fieldValues[NewRecord.fieldValues.Count + PATH_OFFSET] += itemInfo.Item1 + "-->";

            // put the new select elements into new record.
            var startOfSelectElementsIdx = nodeIdx + metaHeaderLength;
            for (var i = startOfSelectElementsIdx; i < NewRecord.fieldValues.Count; i++)
            {
                if (NewRecord.RetriveData(i) == "" && itemInfo.Item3[i] != "")
                    NewRecord.fieldValues[i] = itemInfo.Item3[i];
            }
            return NewRecord;
        }
    }

    /// <summary>
    /// TraversalOperator is used to traval a graph pattern and return asked result.
    /// TraversalOperator.Next() returns one result of what its specifier specified.
    /// By connecting TraversalProcessor together it returns the final result.
    /// </summary>
    internal class TraversalOperator : TraversalBaseOperator
    {
        // Buffer on both input and output sides.
        private Queue<RawRecord> InputBuffer;
        private Queue<RawRecord> OutputBuffer;
        private int InputBufferSize;
        private int OutputBufferSize;
        // The operator that gives the current operator input.
        internal GraphViewExecutionOperator inputOperator;

        // The starting index of the vertex in the input record from which the traversal starts
        internal int src;
        internal int srcAdj;
        // The index of the destination node in the record
        internal int dest;
        // Segement of DocDb script for querying.
        // Containing complete SELECT/FROM/MATCH clause and partial WHERE clause (IN clause should be construct below)
        private DocDbScript docDbScript;
        // Reverse check list, using to indicate which two field should be compared when performing JOIN 
        private Dictionary<int, int> checkList;
        // The operator of a SELECT query defining a single step of a path expression
        internal GraphViewExecutionOperator pathStepOperator;

        // A mark to tell whether the traversal go through normal edge or reverse edge. 
        private bool IsReverse; 

        internal TraversalOperator(GraphViewConnection pConnection, GraphViewExecutionOperator pChildProcessor, DocDbScript pScript, int pSrc, int pSrcAdj, int pDest, List<string> pheader, int pMetaHeaderLength, Dictionary<int, int> pCheckList, int pInputBufferSize, int pOutputBufferSize, bool pReverse, GraphViewExecutionOperator pInternalOperator = null, BooleanFunction pBooleanCheck = null)
        {
            this.Open();
            inputOperator = pChildProcessor;
            connection = pConnection;
            docDbScript = pScript;
            InputBufferSize = pInputBufferSize;
            OutputBufferSize = pOutputBufferSize;
            InputBuffer = new Queue<RawRecord>();
            OutputBuffer = new Queue<RawRecord>();
            src = pSrc;
            srcAdj = pSrcAdj;
            dest = pDest;
            checkList = pCheckList;
            metaHeaderLength = pMetaHeaderLength;
            header = pheader;
            crossDocumentJoinPredicates = pBooleanCheck;
            pathStepOperator = pInternalOperator;
            IsReverse = pReverse;
            if (pathStepOperator != null) pathStepOperator = (pathStepOperator as OutputOperator).ChildOperator;
        }

        public override RawRecord Next()
        {
            // If the output buffer is not empty, returns a result.
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (inputOperator != null && !inputOperator.State())))
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }

            // Fills the input buffer by pulling from the input operator
            while (InputBuffer.Count() < InputBufferSize && inputOperator.State())
            {
                if (inputOperator != null && inputOperator.State())
                {
                    RawRecord Result = inputOperator.Next();
                    if (Result == null)
                    {
                        inputOperator.Close();
                    }
                    else
                    {
                        InputBuffer.Enqueue(Result);
                    }
                }
            }

            // Consturct the "IN" clause
            if (InputBuffer.Count != 0)
            {
                // To deduplicate edge reference from record in the input buffer, and put them into in clause. 
                HashSet<string> EdgeRefSet = new HashSet<string>();
                // Temp table generated by the cross apply process
                List<RawRecord> inputRecords = new List<RawRecord>();
                var decoder = new DocDbDecoder(); 
                if (pathStepOperator == null)
                {
                    foreach (RawRecord record in InputBuffer)
                        decoder.CrossApplyEdge(record, ref EdgeRefSet, ref inputRecords, header, srcAdj, dest, metaHeaderLength);
                }
                else
                {
                    // Replace the root operator with constant source operator.
                    TraversalOperator root = pathStepOperator as TraversalOperator;
                    while (!(root.inputOperator is FetchNodeOperator || root.inputOperator is ConstantSourceOperator))
                        root = root.inputOperator as TraversalOperator;
                    ConstantSourceOperator source = new ConstantSourceOperator(null);
                    root.inputOperator = source;

                    foreach (RawRecord record in InputBuffer)
                        decoder.CrossApplyPath(record, pathStepOperator, source, ref EdgeRefSet,
                            ref inputRecords, header, src, srcAdj, dest, metaHeaderLength);
                }

                // Skip redundant SendQuery when there is no adj in the InputBuffer
                EdgeRefSet.Remove("");
                if (EdgeRefSet.Count > 0)
                { 
                    List<WScalarExpression> sinkIdList = EdgeRefSet.Select(edge => new WValueExpression {SingleQuoted = true, Value = edge}).Cast<WScalarExpression>().ToList();
                    WInPredicate sinkCheck = new WInPredicate
                    {
                        Expression = new WColumnReferenceExpression(header[dest], "id"),
                        Values = sinkIdList
                    };
                    docDbScript.WhereClause.SearchCondition =
                        WBooleanBinaryExpression.Conjunction(docDbScript.WhereClause.SearchCondition, sinkCheck);

                    // Send query to server and decode the result.
                    try
                    {
                        //HashSet<string> UniqueRecord = new HashSet<string>();
                        var script = docDbScript.ToString();
                        var sinkNodeCollection = (IQueryable<dynamic>)SendQuery(script, connection);
                        var results = decoder.DecodeJObjects(sinkNodeCollection.ToList(), header, dest, metaHeaderLength);
                        var joinedRecords = new List<RawRecord>();
                        foreach (var item in results)
                        {
                            var sinkId = item.Item1;
                            // Join the old record with the new one if edge.sinkId = item.Id
                            foreach (var oldRecord in inputRecords)
                            {
                                if (oldRecord.fieldValues[srcAdj+1].Equals(sinkId))
                                    joinedRecords.Add(ConstructRawRecord(item, oldRecord, header, dest, metaHeaderLength));
                            }
                        }

                        var allJoinedRecords = new List<RawRecord>();

                        // Join all other edges
                        if (pathStepOperator != null)
                            allJoinedRecords = joinedRecords;
                        else
                        {
                            // srcAdj edge has been joined before
                            checkList.Remove(srcAdj);
                            
                            if (checkList.Any())
                            {
                                var tmpSet = new HashSet<string>();
                                foreach (var pair in checkList)
                                {
                                    var adjIdx = pair.Key;
                                    var joinDestIdx = pair.Value;

                                    foreach (var record in joinedRecords)
                                    {
                                        decoder.CrossApplyEdge(record, ref tmpSet, ref allJoinedRecords, header,
                                            adjIdx, joinDestIdx);
                                    }
                                }
                            }
                            else
                                allJoinedRecords = joinedRecords;
                        }

                        foreach (var newRecord in allJoinedRecords)
                        {
                            // If the cross document join successed, put the record into output buffer. 
                            if (RecordFilter(crossDocumentJoinPredicates, newRecord))
                                OutputBuffer.Enqueue(newRecord);
                        }
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                }
            }
            InputBuffer.Clear();
            if (OutputBuffer.Count <= 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        private class PathRecord
        {
            public RawRecord PathRec { get; set; }
            public string SinkId { get; set; }
        }

        /// <summary>
        /// Given a vertex (triple) record, the function computes all paths starting from it. 
        /// The step of a path is defined by a SELECT subquery, describing a single edge
        /// or multiple edges. 
        /// </summary>
        /// <param name="sourceRecord">The input vertex record</param>
        /// <returns>A list of path-sink pairs</returns>
        private Queue<PathRecord> PathFunction(RawRecord sourceRecord, ref ConstantSourceOperator source)
        {
            // A list of paths discovered
            Queue<PathRecord> allPaths = new Queue<PathRecord>();
            // A list of paths discovered in last iteration
            Queue<PathRecord> mostRecentlyDiscoveredPaths = new Queue<PathRecord>();
            //// A list of paths newly discovered in current iteration
            //Queue<Tuple<RawRecord, string>> newlyDiscoveredPaths = new Queue<Tuple<RawRecord, string>>();

            mostRecentlyDiscoveredPaths.Enqueue(new PathRecord() {
                PathRec = sourceRecord,
                SinkId = sourceRecord.fieldValues[1]
            });

            allPaths.Enqueue(new PathRecord()
            {
                PathRec = sourceRecord,
                SinkId = sourceRecord.fieldValues[1]
            });

            pathStepOperator.ResetState();

            while (mostRecentlyDiscoveredPaths.Count > 0)
            {
                PathRecord start = mostRecentlyDiscoveredPaths.Dequeue();
                int lastVertexIndex = start.PathRec.fieldValues.Count - 4;

                var srecord = new RawRecord(0);

                // Put the start node in the Kth queue back to the constant source 
                srecord.fieldValues.Add(start.PathRec.fieldValues[lastVertexIndex]);
                srecord.fieldValues.Add(start.PathRec.fieldValues[lastVertexIndex + 1]);
                srecord.fieldValues.Add(start.PathRec.fieldValues[lastVertexIndex + 2]);
                srecord.fieldValues.Add(start.PathRec.fieldValues[lastVertexIndex + 3]);
                source.ConstantSource = srecord;
                // reset state of internal operator
                pathStepOperator.ResetState();
                // Put all the results back into (K+1)th queue.
                while (pathStepOperator.State())
                {
                    var EndRecord = pathStepOperator.Next();
                    if (EndRecord != null)
                    {
                        lastVertexIndex = EndRecord.fieldValues.Count - 4;
                        var sink = EndRecord.fieldValues[lastVertexIndex + 1];
                        if (sink != "")
                        {
                            PathRecord newPath = new PathRecord()
                            {
                                PathRec = EndRecord,
                                SinkId = sink
                            };
                            mostRecentlyDiscoveredPaths.Enqueue(newPath);
                            allPaths.Enqueue(newPath);
                        }
                    }
                }
            }
            return allPaths;
        }

        public override void ResetState()
        {
            this.Open();
            if (docDbScript?.WhereClause != null)
                docDbScript.WhereClause.SearchCondition = docDbScript.OriginalSearchCondition;
            inputOperator.ResetState();
        }
    }

    /// <summary>
    /// FetchNodeOperator is used to fetch a certain node.
    /// FetchNodeOperator.Next() returns one result of what its specifier specified.
    /// It often used as the input of a traversal operator
    /// </summary>
    internal class FetchNodeOperator : TraversalBaseOperator
    {
        private Queue<RawRecord> OutputBuffer;
        private int nodeIdx;
        private DocDbScript docDbScript;

        public FetchNodeOperator(GraphViewConnection pConnection, DocDbScript pScript, int pnode, List<string> pheader, int pMetaHeaderLength, int pOutputBufferSize, GraphViewExecutionOperator pChildOperator = null)
        {
            this.Open();
            connection = pConnection;
            docDbScript = pScript;
            header = pheader;
            nodeIdx = pnode;
            metaHeaderLength = pMetaHeaderLength;
        }

        public override RawRecord Next()
        {
            // Set up output buffer
            if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1)
                    this.Close();
                return OutputBuffer.Dequeue();
            }

            string script = docDbScript.ToString();
            RawRecord oldRecord = new RawRecord(header.Count);
            try
            {   // Send query to the server
                var nodes = (IQueryable<dynamic>) SendQuery(script, connection);
                var decoder = new DocDbDecoder();
                var results = decoder.DecodeJObjects(nodes.ToList(), header, 0, metaHeaderLength);
                // Decode the result retrived from server and generate new record
                foreach (var item in results)
                {
                    RawRecord newRecord = ConstructRawRecord(item, oldRecord, header, nodeIdx, metaHeaderLength);
                    OutputBuffer.Enqueue(newRecord);
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }

            // Close output buffer
            if (OutputBuffer.Count <= 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }
        public override void ResetState()
        {
            this.Open();
        }
    }

    /// <summary>
    /// CartesianProductOperator is used to generate cartesian product of the record from different sub graphs
    /// CartesianProductOperator.Next() returns the combination of all the records that generated from giving operators.
    /// As example, the cartesian product of 
    /// |   A   |  empty |   B   |
    /// and 
    /// | empty |    C   | empty |
    /// will be
    /// |   A   |    C   |   B   |
    /// </summary>
    internal class CartesianProductOperator : GraphViewExecutionOperator
    {
        private List<GraphViewExecutionOperator> OperatorOnSubGraphs;

        private Queue<RawRecord> OutputBuffer;

        internal BooleanFunction BooleanCheck;
        public CartesianProductOperator(List<GraphViewExecutionOperator> pProcessorOnSubGraph, List<string> pheader)
        {
            this.Open();
            header = pheader;
            OperatorOnSubGraphs = pProcessorOnSubGraph;
        }

        override public RawRecord Next()
        {
           if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }

            List<List<RawRecord>> ResultsFromChildrenOperator = new List<List<RawRecord>>();
            
            // Load all the records from its child operators.

            foreach (var ChildOperator in OperatorOnSubGraphs)
            {
                ResultsFromChildrenOperator.Add(new List<RawRecord>());
                RawRecord result = ChildOperator.Next();
                while (result != null && ChildOperator.State())
                {
                    ResultsFromChildrenOperator.Last().Add(result);
                    result = ChildOperator.Next();
                }
                if (result != null && RecordFilter(result)) ResultsFromChildrenOperator.Last().Add(result);
            }
            // Do catesian product on all the records from every subgraphs.
            if (OperatorOnSubGraphs.Count != 0) CartesianProductOnRecord(ResultsFromChildrenOperator, 0, new RawRecord(header.Count));
            OperatorOnSubGraphs.Clear();
            if (OutputBuffer.Count < 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }
        private bool RecordFilter(RawRecord r)
        {
            if (BooleanCheck == null) return true;
            else return BooleanCheck.Evaluate(r);
        }

        // Find the cartesian product of giving sets of records.
        private void CartesianProductOnRecord(List<List<RawRecord>> RecordSet, int IndexOfOperator, RawRecord result)
        {
            if (IndexOfOperator == RecordSet.Count)
            {
                OutputBuffer.Enqueue(result);
                return;
            }
            foreach (var record in RecordSet[IndexOfOperator])
            {
                RawRecord NewResult = new RawRecord(result);
                for (int i = 0; i < OperatorOnSubGraphs[IndexOfOperator].header.Count; i++)
                {
                    if (NewResult.RetriveData(header, OperatorOnSubGraphs[IndexOfOperator].header[i]) == "" && record.fieldValues[i] != "")
                        NewResult.fieldValues[header.IndexOf(OperatorOnSubGraphs[IndexOfOperator].header[i])] = record.fieldValues[i];
                }
                CartesianProductOnRecord(RecordSet, IndexOfOperator + 1, NewResult);
            }
        }
    }

    /// <summary>
    /// UnionOperator is used for "union" keyword in Gremlin, that requires to union the output of several pipeline 
    /// </summary>
    internal class ConcatenateOperator : GraphViewExecutionOperator
    {
        internal List<GraphViewExecutionOperator> Sources;
        internal int FromWhichSource;
        internal RawRecord result;
        public ConcatenateOperator(List<GraphViewExecutionOperator> pSources)
        {
            this.Open();
            Sources = pSources;
            FromWhichSource = 0;
        }

        override public RawRecord Next()
        {
            // It will consume each child operator of itself in turns. 
            if (!Sources[FromWhichSource].State() || (result = Sources[FromWhichSource].Next()) == null)
            {
                if (FromWhichSource == Sources.Count)
                {
                    this.Close();
                    return null;
                }
                FromWhichSource++;
                return Sources[FromWhichSource].Next();
            }
            else
            {
                return Sources[FromWhichSource].Next();
            }
        }
    }
    /// <summary>
    /// CoalesceOperator is used for "coalesce" keyword in Gremlin, it will try to acquire output from each child operator of itself
    /// until any one of them provides an output, it will take the output and close the operators after the one.
    /// </summary>
    internal class CoalesceOperator : GraphViewExecutionOperator
    {
        internal List<GraphViewExecutionOperator> Sources;
        internal int FromWhichSource;
        internal RawRecord result;
        internal int CoalesceNumber;
        public CoalesceOperator(List<GraphViewExecutionOperator> pSources, int pCoalesceNumber)
        {
            this.Open();
            Sources = pSources;
            FromWhichSource = 0;
            CoalesceNumber = pCoalesceNumber;
            header = new List<string>();
            foreach(var x in Sources)
                if (x is OutputOperator) header = header.Concat((x as OutputOperator).SelectedElement).ToList();
        }

        override public RawRecord Next()
        {
            HashSet<string> ResultSet = new HashSet<string>();
            while (FromWhichSource < Sources.Count && (!Sources[FromWhichSource].State() || (result = Sources[FromWhichSource].Next()) == null))
                FromWhichSource++;
            if (FromWhichSource == Sources.Count)
            {
                this.Close();
                return null;
            }
            else
            {
                for (int i = FromWhichSource + 1; i < Sources.Count; i++) Sources[i].Close();
                string Temp = "";
                header = Sources[FromWhichSource].header;
                return result;
            }
        }
    }
    /// <summary>
    /// Orderby operator is used for orderby clause. It will takes all the output of its child operator and sort them by a giving key.
    /// </summary>
    internal class OrderbyOperator : GraphViewExecutionOperator
    {
        internal GraphViewExecutionOperator ChildOperator;
        internal List<RawRecord> results;
        internal Queue<RawRecord> ResultQueue;
        // By what key to order.
        internal string bywhat;
        // <key, order>
        internal List<Tuple<string, SortOrder>> OrderByElements; 

        public OrderbyOperator(GraphViewExecutionOperator pChildOperator, List<Tuple<string, SortOrder>> pOrderByElements, List<string> pheader)
        {
            this.Open();
            header = pheader;
            ChildOperator = pChildOperator;
            OrderByElements = pOrderByElements;
        }

        public override RawRecord Next()
        {
            if (results == null)
            {
                results = new List<RawRecord>();
                while (ChildOperator.State())
                {
                    RawRecord Temp = ChildOperator.Next();
                    if (Temp != null)
                        results.Add(Temp);
                }
                results.Sort((x, y) =>
                {
                    var ret = 0;
                    foreach (var orderByElement in OrderByElements)
                    {
                        var expr = orderByElement.Item1;
                        var sortOrder = orderByElement.Item2;
                        if (sortOrder == SortOrder.Ascending || sortOrder == SortOrder.NotSpecified)
                            ret = string.Compare(x.RetriveData(header, expr), y.RetriveData(header, expr),
                                StringComparison.OrdinalIgnoreCase);
                        else if (sortOrder == SortOrder.Descending)
                            ret = string.Compare(y.RetriveData(header, expr), x.RetriveData(header, expr),
                                StringComparison.OrdinalIgnoreCase);
                        if (ret != 0) break;
                    }
                    return ret;
                });

                ResultQueue = new Queue<RawRecord>();
                foreach (var x in results)
                    ResultQueue.Enqueue(x);
            }
            if (ResultQueue.Count <= 1) this.Close();
            if (ResultQueue.Count != 0) return ResultQueue.Dequeue();
            return null;
        }
    }
    /// <summary>
    /// Output operator will take output from its child operator and cut the internal information.
    /// </summary>
    internal class OutputOperator : GraphViewExecutionOperator
    {
        internal GraphViewExecutionOperator ChildOperator;
        // what element is selected and needed to be output.
        internal List<string> SelectedElement;
        // whether to output path of the traversal
        internal bool OutputPath;

        public OutputOperator(GraphViewExecutionOperator pChildOperator, List<string> pSelectedElement, List<string> pHeader)
        {
            this.Open();
            ChildOperator = pChildOperator;
            SelectedElement = pSelectedElement;
            header = pHeader;
        }

        public OutputOperator(GraphViewExecutionOperator pChildOperator, 
            bool pOutputPath, List<string> pHeader)
        {
            this.Open();
            ChildOperator = pChildOperator;
            OutputPath = pOutputPath;
            header = pHeader;
            SelectedElement = new List<string>() { "PATH" };
        }

        override public RawRecord Next()
        {
            if (OutputPath)
            {
                SelectedElement = new List<string>() {"PATH"};
                RawRecord OutputRecord = new RawRecord(1);
                RawRecord InputRecord = null;
                if (ChildOperator.State())
                {
                    while ((InputRecord = ChildOperator.Next()) == null && ChildOperator.State()) ;
                    if (!ChildOperator.State())
                    {
                        this.Close();
                    }
                    if (InputRecord != null)
                    {
                        OutputRecord.fieldValues[0] = CutTheTail(InputRecord.fieldValues.Last(),3);
                        return OutputRecord;
                    }
                    else return null;
                }
            }
            else if (SelectedElement != null)
            {
                RawRecord OutputRecord = new RawRecord(SelectedElement.Count);
                RawRecord InputRecord = null;
                if (ChildOperator.State())
                {
                    while ((InputRecord = ChildOperator.Next()) == null && ChildOperator.State()) ;
                    if (!ChildOperator.State())
                    {
                        this.Close();
                    }
                    if (InputRecord != null)
                    {
                        foreach (var x in SelectedElement)
                            OutputRecord.fieldValues[SelectedElement.IndexOf(x)] = InputRecord.RetriveData(ChildOperator.header, x);
                        return OutputRecord;
                    }
                    else return null;
                }
                else
                {
                    this.Close();
                    return null;
                }
            }
            return null;
        }

        string CutTheTail(string InRangeScript,int len = 1)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - len);
        }
    }

    internal class ConstantSourceOperator : GraphViewExecutionOperator
    {
        public RawRecord ConstantSource
        {
            get { return this.ConstantSource; }
            set { this.ConstantSource = value; this.Open(); }
        }

        public ConstantSourceOperator(RawRecord pConstant)
        {
            ConstantSource = pConstant;
            this.Open();
        }

        public void SetRef(RawRecord pConstant)
        {
            ConstantSource = pConstant;
            this.Open();
        }

        override public RawRecord Next()
        {
            this.Close();
            return ConstantSource;
        }

    }

    public class GraphViewDataReader : IDataReader
    {
        private GraphViewExecutionOperator DataSource;
        private DMultiPartIdentifierParser Parser;
        RawRecord CurrentRecord;
        internal GraphViewDataReader(GraphViewExecutionOperator pDataSource)
        {
            DataSource = pDataSource;
            FieldCount = DataSource.header.Count;
            Parser = new DMultiPartIdentifierParser();
        }
        public bool Read()
        {
            CurrentRecord = DataSource.Next();
            if (CurrentRecord != null) return true;
            else return false;
        }
        public object this[string FieldName]
        {
            get
            {
                var identifier = Parser.ParseMultiPartIdentifier(FieldName);
                var fieldName = identifier != null ? identifier.ToSqlStyleString() : FieldName;
                if (DataSource != null)
                    return CurrentRecord.RetriveData((DataSource as OutputOperator).SelectedElement, fieldName);
                else throw new IndexOutOfRangeException("No data source");
            }
        }
        public object this[int index]
        {
            get
            {
                return CurrentRecord.RetriveData(index);
            }
        }

        public int Depth { get; set; }
        public bool IsClosed { get; set; }
        public int RecordsAffected { get; set; }
        public int FieldCount { get; set; }
        public void Close() { throw new NotImplementedException(); }
        public void Dispose() { throw new NotImplementedException(); }
        public bool GetBoolean(int x) { throw new NotImplementedException(); }
        public byte GetByte(Int32 x) { throw new NotImplementedException(); }
        public long GetBytes(Int32 x, Int64 y, Byte[] z, Int32 w, Int32 u) { throw new NotImplementedException(); }
        public char GetChar(Int32 x) { throw new NotImplementedException(); }
        public long GetChars(Int32 x, Int64 y, Char[] z, Int32 w, Int32 u) { throw new NotImplementedException(); }
        public IDataReader GetData(Int32 x) { throw new NotImplementedException(); }
        public string GetDataTypeName(Int32 x) { throw new NotImplementedException(); }
        public DateTime GetDateTime(Int32 x) { throw new NotImplementedException(); }
        public decimal GetDecimal(Int32 x) { throw new NotImplementedException(); }
        public double GetDouble(Int32 x) { throw new NotImplementedException(); }
        public Type GetFieldType(Int32 x) { throw new NotImplementedException(); }
        public float GetFloat(Int32 x) { throw new NotImplementedException(); }
        public Guid GetGuid(Int32 x) { throw new NotImplementedException(); }
        public Int16 GetInt16(Int32 x) { throw new NotImplementedException(); }
        public Int32 GetInt32(Int32 x) { throw new NotImplementedException(); }
        public Int64 GetInt64(Int32 x) { throw new NotImplementedException(); }
        public DataTable GetSchemaTable() { throw new NotImplementedException(); }
        public string GetName(Int32 x) { return DataSource.header[x]; }
        public int GetOrdinal(string x) { throw new NotImplementedException(); }
        public string GetString(Int32 x) { return CurrentRecord.RetriveData(x); }
        public object GetValue(Int32 x) { return CurrentRecord.RetriveData(x); }
        public int GetValues(object[] x) { throw new NotImplementedException(); }
        public bool IsDBNull(Int32 x) { throw new NotImplementedException(); }
        public bool NextResult() { throw new NotImplementedException(); }
    }
}

