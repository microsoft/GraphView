using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
// Add DocumentDB references
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// Base class of the traversal operator and the fetch-node operator.
    /// providing common properties and utility functions. 
    /// </summary>
    internal abstract class TraversalBaseOperator : GraphViewExecutionOperator
    {
        // Predicates of cross-document joins, excluding joins between edge references and vertex ID's. 
        internal BooleanFunction crossDocumentJoinPredicates;

        internal GraphViewConnection connection;

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

        /// <summary>
        /// Constructs a new record produced by this operator
        /// </summary>
        /// <param name="NumberOfProcessedNode"></param>
        /// <param name="ItemInfo">Info of the node newly processed by this operator</param>
        /// <param name="OldRecord">The input record of the operator</param>
        /// <param name="header"></param>
        /// <param name="addpath">Whether to produce a path string for all the nodes processed so far</param>
        /// <returns>The record to be returned</returns>
        internal static RawRecord ConstructRawRecord(int NumberOfProcessedNode, Tuple<string, string, string, List<string>> ItemInfo, RawRecord OldRecord, List<string> header, string appendedPath = "")
        {
            // copy the old internal information into new record
            RawRecord NewRecord = new RawRecord(header.Count);
            for (int i = 0; i < NumberOfProcessedNode * 3; i++)
                NewRecord.fieldValues[i] = OldRecord.fieldValues[i];
            // put the nodes into new record.
            NewRecord.fieldValues[NumberOfProcessedNode * 3] = ItemInfo.Item1;
            NewRecord.fieldValues[NumberOfProcessedNode * 3 + ADJ_OFFSET] = ItemInfo.Item2;
            NewRecord.fieldValues[NumberOfProcessedNode * 3 + REV_ADJ_OFFSET] = ItemInfo.Item3;
            for (int i = NumberOfProcessedNode * 3; i < header.Count - 3; i++)
                NewRecord.fieldValues[i + 3] = OldRecord.fieldValues[i];
            // extend the path if needed.
            if (appendedPath != "") NewRecord.fieldValues[NewRecord.fieldValues.Count + PATH_OFFSET] = appendedPath;
            NewRecord.fieldValues[NewRecord.fieldValues.Count + PATH_OFFSET] += ItemInfo.Item1 + "-->";
            // put the result elements that are consturcted before into new record.
            for (int i = NumberOfProcessedNode * 3 + 3; i < NewRecord.fieldValues.Count; i++)
            {
                if (NewRecord.RetriveData(i) == "" && ItemInfo.Item4[i] != "")
                    NewRecord.fieldValues[i] = ItemInfo.Item4[i];
            }
            return NewRecord;
        }
        // Check whether the giving record r satisfy the giving boolean check function. 
        internal static bool RecordFilter(BooleanFunction BooleanCheck, RawRecord r)
        {
            if (BooleanCheck == null) return true;
            else return BooleanCheck.eval(r);
        }
        // Decode JObject into (id, adjacent list, reverse adjacent list, selected elements) quadruple
        internal static Tuple<string, string, string, List<string>> DecodeJObject(JObject Item, List<string> header, int StartOfResultField)
        {
            JToken NodeInfo = ((JObject)Item)["NodeInfo"];
            JToken id = NodeInfo["id"];
            JToken edge = ((JObject)NodeInfo)["edge"];
            JToken reverse = ((JObject)NodeInfo)["reverse"];
            string ReverseEdgeID = "";
            foreach (var x in reverse)
            {
                ReverseEdgeID += "\"" + x["_sink"] + "\"" + ",";
            }
            string EdgeID = "";
            foreach (var x in edge)
            {
                EdgeID += "\"" + x["_sink"] + "\"" + ",";
            }
            // Generate the result list that need to be joined with the original one
            // ... |     SELECTED ELEMENT 1 ("A_NAME")  |   SELECTED ELEMENT 2 ("A"_AGE") |    SELECTED ELEMENT  3 ("B_AGE")   |
            //                  a.name                             a.age                    Not found when dealing with node A
            RawRecord ResultRecord = new RawRecord(header.Count);
            foreach (
                string ResultFieldName in header.GetRange(StartOfResultField, header.Count - StartOfResultField))
            {
                string result = "";
                // Alias with "." is illegal in documentDB, so all the "." in alias will be replaced by "_".
                if (Item[ResultFieldName.Replace(".", "_")] != null)
                    result = (Item)[ResultFieldName.Replace(".", "_")].ToString();
                ResultRecord.fieldValues[header.IndexOf(ResultFieldName)] = result;
            }
            return new Tuple<string, string, string,List<string>>(id.ToString(), CutTheTail(EdgeID), CutTheTail(ReverseEdgeID), ResultRecord.fieldValues);
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

        // Segement of DocDb script for querying.
        // Containing complete SELECT/FROM/MATCH clause and partial WHERE clause (IN clause should be construct below)
        private string docDbScript;

        // A list of forward/backward edges from the traversal's sink pointing to the vertices produced by the input operator
        // A vertex produced by the input operator is identified by the index in the input record
        private List<Tuple<int,string>> CheckList;
        
        // The operator of a SELECT query defining a single step of a path expression
        internal GraphViewExecutionOperator pathStepOperator;

        // A mark to tell whether the traversal go through normal edge or reverse edge. 
        private bool IsReverse; 

        internal TraversalOperator(GraphViewConnection pConnection, GraphViewExecutionOperator pChildProcessor, string pScript, int pSrc, List<string> pheader, List<Tuple<int, string>> pCheckList, int pNumberOfProcessedVertices, int pInputBufferSize, int pOutputBufferSize, bool pReverse, GraphViewExecutionOperator pInternalOperator = null, BooleanFunction pBooleanCheck = null)
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
            CheckList = pCheckList;
            header = pheader;
            NumberOfProcessedVertices = pNumberOfProcessedVertices;
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

            string script = docDbScript;

            // The collection maps each input record to a list of paths starting from it. 
            // Each paths is in a <sink, path_string> pair. 
            Dictionary<RawRecord, List<Tuple<string, string>>> pathCollection = new Dictionary<RawRecord, List<Tuple<string, string>>>();

            // Consturct the "IN" clause
            if (InputBuffer.Count != 0)
            {
                string sinkIdValueList = "";

                // To deduplicate edge reference from record in the input buffer, and put them into in clause. 
                HashSet<string> EdgeRefSet = new HashSet<string>();
                if (pathStepOperator == null)
                {
                    foreach (RawRecord record in InputBuffer)
                    {
                        var adj = record.RetriveData(src + (IsReverse ? REV_ADJ_OFFSET : ADJ_OFFSET)).Split(',');
                        foreach (var edge in adj)
                        {
                            if (edge != "" && !EdgeRefSet.Contains(edge))
                                sinkIdValueList += edge + ",";
                            EdgeRefSet.Add(edge);
                        }
                    }
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
                    {
                        var inputRecord = new RawRecord(0);
                        // Extracts the triple of the starting node from the input record 
                        inputRecord.fieldValues.Add(record.fieldValues[(NumberOfProcessedVertices - 1) * 3]);
                        inputRecord.fieldValues.Add(record.fieldValues[(NumberOfProcessedVertices - 1) * 3 + 1]);
                        inputRecord.fieldValues.Add(record.fieldValues[(NumberOfProcessedVertices - 1) * 3 + 2]);
                        inputRecord.fieldValues.Add(record.fieldValues[record.fieldValues.Count - 1]);
                        // put it into path function
                        var PathResult = PathFunction(inputRecord, ref source);
                        // sink and corresponding path.
                        List<Tuple<string,string>> pathList = new List<Tuple<string, string>>();
                        foreach (var x in PathResult)
                        {
                            pathList.Add(new Tuple<string, string>(x.SinkId, x.PathRec.fieldValues[x.PathRec.fieldValues.Count - 1]));
                            var adj = x.SinkId.Split(',');
                            foreach (var edge in adj)
                            {
                                if (edge != "" && !EdgeRefSet.Contains(edge))
                                    sinkIdValueList += edge + ",";
                                EdgeRefSet.Add(edge);
                            }
                        }

                        pathCollection.Add(record, pathList);
                    }
                }
                sinkIdValueList = CutTheTail(sinkIdValueList);
                // Skip redundant SendQuery when there is no adj in the InputBuffer
                if (sinkIdValueList != "")
                {
                    if (!script.Contains("WHERE"))
                        script += " WHERE " + header[NumberOfProcessedVertices * 3] + ".id IN (" + sinkIdValueList + ")";
                    else script += " AND " + header[NumberOfProcessedVertices * 3] + ".id IN (" + sinkIdValueList + ")";

                    if (pathStepOperator == null)
                    {
                        foreach (var reverseEdge in CheckList)
                        {
                            EdgeRefSet.Clear();
                            sinkIdValueList = "";
                            foreach (RawRecord record in InputBuffer)
                            {
                                var adj = record.RetriveData(reverseEdge.Item1).Split(',');
                                foreach (var edge in adj)
                                {
                                    if (edge != "" && !EdgeRefSet.Contains(edge))
                                        sinkIdValueList += "\"" + edge + "\"" + ",";
                                    EdgeRefSet.Add(edge);
                                }
                            }
                            sinkIdValueList = CutTheTail(sinkIdValueList);
                            // Remove the "_REV" tail
                            if (!script.Contains("WHERE"))
                                script += " WHERE " + CutTheTail(reverseEdge.Item2, 4) + "._sink IN (" + sinkIdValueList + ")";
                            else script += " AND " + CutTheTail(reverseEdge.Item2, 4) + "._sink IN (" + sinkIdValueList + ")";
                        }
                    }

                    // Send query to server and decode the result.
                    try
                    {
                        HashSet<string> UniqueRecord = new HashSet<string>();
                        IQueryable<dynamic> sinkNodeCollection = (IQueryable<dynamic>)SendQuery(script, connection);
                        foreach (var sinkJsonObject in sinkNodeCollection)
                        {
                            // Decode some information that describe the found node.
                            Tuple<string, string, string, List<string>> sinkVertex = DecodeJObject((JObject)sinkJsonObject, header, NumberOfProcessedVertices * 3);
                            string vertexId = sinkVertex.Item1;

                            // If it is a path traversal, matches the returned sink vertex against every source vertex and their outgoing paths. 
                            // A new record is constructed and returned when the sink vertex happens to be the last vertex of a path. 
                            // No reverse check can be performed here.
                            if (pathStepOperator != null)
                            {
                                foreach (RawRecord sourceRec in pathCollection.Keys)
                                {
                                    foreach (Tuple<string, string> pathTuple in pathCollection[sourceRec])
                                    {
                                        string pathSink = pathTuple.Item1;
                                        if (pathSink.Contains(vertexId))
                                        {
                                            RawRecord NewRecord = ConstructRawRecord(NumberOfProcessedVertices, sinkVertex,
                                                sourceRec, header,
                                                pathTuple.Item2);
                                            if (RecordFilter(crossDocumentJoinPredicates, NewRecord))
                                                OutputBuffer.Enqueue(NewRecord);
                                        }
                                    }
                                }
                            }
                            else
                                // For normal edge, join the old record with the new one if checked valid.
                                foreach (var record in InputBuffer)
                                {
                                    // reverse check
                                    bool ValidFlag = true;
                                    if (CheckList != null)
                                        foreach (var neighbor in CheckList)
                                        {
                                            // Alignment and reverse checking.
                                            string edge = (((JObject)sinkJsonObject)[neighbor.Item2])["_sink"].ToString();
                                            if (edge != record.RetriveData(neighbor.Item1))
                                            {
                                                ValidFlag = false;
                                                break;
                                            }
                                        }
                                    if (ValidFlag)
                                    {
                                        // If aligned and reverse checked vailied, join the old record with the new one.
                                        RawRecord NewRecord = ConstructRawRecord(NumberOfProcessedVertices, sinkVertex, record,
                                            header);
                                        // If the cross document join successed, put the record into output buffer. 
                                        if (RecordFilter(crossDocumentJoinPredicates, NewRecord))
                                            OutputBuffer.Enqueue(NewRecord);
                                    }
                                }
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

        private string docDbScript;

        public FetchNodeOperator(GraphViewConnection pConnection, string pScript, int pnode, List<string> pheader, int pNumberOfProcessedVertices, int pOutputBufferSize, GraphViewExecutionOperator pChildOperator = null)
        {
            this.Open();
            connection = pConnection;
            docDbScript = pScript;
            header = pheader;
            NumberOfProcessedVertices = pNumberOfProcessedVertices;
        }
        override public RawRecord Next()
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
            string script = docDbScript;
            RawRecord OldRecord = new RawRecord(header.Count);

            try
            {   // Send query to the server
                IQueryable<dynamic> Node = (IQueryable<dynamic>) SendQuery(script, connection);
                // Decode the result retrived from server and generate new record
                foreach (var item in Node)
                {
                    Tuple<string, string, string, List<string>> ItemInfo = DecodeJObject((JObject) item,header, NumberOfProcessedVertices);
                    RawRecord NewRecord = ConstructRawRecord(NumberOfProcessedVertices,ItemInfo, OldRecord, header);
                        if (RecordFilter(crossDocumentJoinPredicates,NewRecord))
                        OutputBuffer.Enqueue(NewRecord);
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
            else return BooleanCheck.eval(r);
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
        internal RawRecord ConstantSource;

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
        RawRecord CurrentRecord;
        internal GraphViewDataReader(GraphViewExecutionOperator pDataSource)
        {
            DataSource = pDataSource;
            FieldCount = DataSource.header.Count;
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
                if (DataSource != null)
                    return CurrentRecord.RetriveData((DataSource as OutputOperator).SelectedElement, FieldName);
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

