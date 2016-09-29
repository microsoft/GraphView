using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Remoting.Messaging;
using System.Security.Policy;
using System.Xml.XPath;
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
        /// <param name="node"></param>
        /// <param name="ItemInfo">Info of the node newly processed by this operator</param>
        /// <param name="OldRecord">The input record of the operator</param>
        /// <param name="header"></param>
        /// <param name="addpath">Whether to produce a path string for all the nodes processed so far</param>
        /// <returns>The record to be returned</returns>
        internal static RawRecord ConstructRawRecord(int NumberOfProcessedNode, Tuple<string, string, string, List<string>> ItemInfo, RawRecord OldRecord, List<string> header, bool addpath)
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
            if (addpath) NewRecord.fieldValues[NewRecord.fieldValues.Count + PATH_OFFSET] += ItemInfo.Item1 + "-->";
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
        string ResultFieldName in
            header.GetRange(StartOfResultField, header.Count - StartOfResultField))
            {
                string result = "";
                // Alias with "." is illegal in documentDB, so all the "." in alias will be replaced by "_".
                if (Item[ResultFieldName.Replace(".", "_")] != null)
                    result = (Item)[ResultFieldName.Replace(".", "_")].ToString();
                ResultRecord.fieldValues[header.IndexOf(ResultFieldName)] = result;
            }
            return new Tuple<string, string, string,List<string>>(id.ToString(), CutTheTail(EdgeID), CutTheTail(ReverseEdgeID),ResultRecord.fieldValues);
        }
        internal static IQueryable<dynamic> SendQuery(string script, GraphViewConnection connection)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DocDB_DatabaseId, connection.DocDB_CollectionId), script, QueryOptions);
            return Result;
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
        internal GraphViewExecutionOperator ChildOperator;

        // Defining from which field in the record does the traversal goes to which field. (Source node defined here, destination node define at base)
        internal int src;

        // Segement of DocDb script for querying.
        // Containing complete SELECT/FROM/MATCH clause and partial WHERE clause (IN clause should be construct below)
        private string docDbScript;

        // Defining which fields should the reverse check have on.
        private List<Tuple<int,string,bool>> CheckList;
        
        // Internal Operator for loop.
        internal GraphViewExecutionOperator InternalOperator;
        // The start of internal operator for path
        private int InternalLoopStartNode;

        // A mark to tell whether the traversal go through normal edge or reverse edge. 
        private bool IsReverse; 

        internal TraversalOperator(GraphViewConnection pConnection, GraphViewExecutionOperator pChildProcessor, string pScript, int pSrc, List<string> pheader, List<Tuple<int, string, bool>> pCheckList, int pNumberOfProcessedVertices, int pInputBufferSize, int pOutputBufferSize, bool pReverse, GraphViewExecutionOperator pInternalOperator = null, BooleanFunction pBooleanCheck = null)
        {
            this.Open();
            ChildOperator = pChildProcessor;
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
            InternalOperator = pInternalOperator;
            IsReverse = pReverse;
            if (InternalOperator != null) InternalOperator = (InternalOperator as OutputOperator).ChildOperator;
        }
        override public RawRecord Next()
        {
            // If the output buffer is not empty, return a result.
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (ChildOperator != null && !ChildOperator.State())))
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }

            // Take input from the output buffer of its child operator.
            while (InputBuffer.Count() < InputBufferSize && ChildOperator.State())
            {
                if (ChildOperator != null && ChildOperator.State())
                {
                    RawRecord Result = (RawRecord)ChildOperator.Next();
                    // If this traversal operator deal with a normal edge, input buffer enqeue the record from its child operator
                    // Otherwise it pass the record to path function to generate new records, and put it into input buffer.
                    if (Result == null) ChildOperator.Close();
                    else InputBuffer.Enqueue(Result);
                }
            }

            string script = docDbScript;
            // Consturct the "IN" clause
            if (InputBuffer.Count != 0)
            {
                string InRangeScript = "";
                // To deduplicate edge reference from record in the input buffer, and put them into in clause. 
                HashSet<string> EdgeRefSet = new HashSet<string>();
                if (InternalOperator == null)
                {
                    foreach (RawRecord record in InputBuffer)
                    {
                        var adj = record.RetriveData(src + (IsReverse ? REV_ADJ_OFFSET : ADJ_OFFSET)).Split(',');
                        foreach (var edge in adj)
                        {
                            if (!EdgeRefSet.Contains(edge))
                                InRangeScript += edge + ",";
                            EdgeRefSet.Add(edge);
                        }
                    }
                }
                else
                {
                    List<RawRecord> PathRecordList = new List<RawRecord>();
                    foreach (RawRecord record in InputBuffer)
                    {
                        var InputRecord = new RawRecord(0);
                        InputRecord.fieldValues.Add(record.fieldValues[(NumberOfProcessedVertices - 1) * 3]);
                        InputRecord.fieldValues.Add(record.fieldValues[(NumberOfProcessedVertices - 1) * 3 + 1]);
                        InputRecord.fieldValues.Add(record.fieldValues[(NumberOfProcessedVertices - 1) * 3 + 2]);
                        InputRecord.fieldValues.Add(record.fieldValues[record.fieldValues.Count - 1]);
                        var PathResult = PathFunction(InputRecord);
                        foreach (var x in PathResult)
                        {
                            RawRecord PathRecord = new RawRecord(record);
                            PathRecord.fieldValues[(NumberOfProcessedVertices - 1)*3 + 1] = x.Item2;
                            PathRecord.fieldValues[PathRecord.fieldValues.Count - 1] = x.Item1.fieldValues[x.Item1.fieldValues.Count - 1];
                            PathRecordList.Add(PathRecord);
                            if (!EdgeRefSet.Contains(x.Item2))
                                InRangeScript += x.Item2 + ",";
                            EdgeRefSet.Add(x.Item2);
                        }
                    }
                    InputBuffer.Clear();
                    foreach (var x in PathRecordList )
                    {
                        InputBuffer.Enqueue(x);
                    }
                }
                InRangeScript = CutTheTail(InRangeScript);
                if (InRangeScript != "")
                    if (!script.Contains("WHERE"))
                        script += "WHERE " + header[NumberOfProcessedVertices * 3] + ".id IN (" + InRangeScript + ")";
                    else script += " AND " + header[NumberOfProcessedVertices * 3] + ".id IN (" + InRangeScript + ")";
                // Send query to server and decode the result.
                try
                {
                    HashSet<string> UniqueRecord = new HashSet<string>();
                    IQueryable<dynamic> Node = (IQueryable<dynamic>) SendQuery(script, connection);
                    foreach (var item in Node)
                    {
                        // Decode some information that describe the found node.
                        Tuple<string, string, string,List<string>> ItemInfo = DecodeJObject((JObject) item,header, NumberOfProcessedVertices * 3);
                        string ID = ItemInfo.Item1;
                        // Join the old record with the new one if checked valid.
                        foreach (var record in InputBuffer)
                        {
                            // reverse check
                            bool ValidFlag = true;
                            // If the dest field already has value, the new ID should be the same with the old one so they can be joined together.
                            // If (record.fieldValues[dest] != "" && record.fieldValues[dest] != ID) continue;
                            if (CheckList != null)
                                foreach (var neighbor in CheckList)
                                {
                                    string edge = (((JObject) item)[neighbor.Item2])["_sink"].ToString();

                                    // Two operations are performed here.
                                    // 
                                    // Alignment:
                                    // As several records's outcoming edges are put into the same query for result,
                                    // each result needs to be align with the record they belongs to.
                                    //|   "NODE1"  |   "NODE1_ADJ"      |   "NODE1_REVADJ"      |  "NODE2"   |   "NODE2_ADJ"      |   "NODE2_REVADJ"      |
                                    // If now we traversal from Node 1 to Node 2.
                                    // Then the Node 2 that we get from the querying result should satisfy that 
                                    // 1) Node 2's id in node 1's adjacent list.
                                    // 2) Node 1's id in node 2's reverse adjacent list.
                                    // so they can be align with each other and join together.
                                    //
                                    // Reverse Checking:
                                    // For those nodes that has been already traversed to, but have link to the dest node.
                                    // Reverse checking have to be performed here.
                                    // |   "NODE1"  |   "NODE1_ADJ"      |   "NODE1_REVADJ"      |  "NODE2"   |   "NODE2_ADJ"      |   "NODE2_REVADJ"      |
                                    // If now we have to perform reverse checking for the edge from node 1 to node 2 on node 2
                                    // Then the Node 2 that we get from the querying result should satisfy that 
                                    // 1) Node 2's id in node 1's reverse adjacent list.
                                    // 2) Node 1's id in node 2's adjacent list.
                                    //
                                    // So the differeces between the two operation is that they are checking different adjacent list.
                                    // And they are controlled by using different offsets. For alignment, it uses ADJ_OFFSET, for reverse checking, it uses REV_ADJ_OFFSET.
                                    if (InternalOperator == null && !(edge == record.RetriveData(neighbor.Item1)))
                                        ValidFlag = false;
                                    // For path traversal operator, no reverse checking can be performed now.
                                    if (InternalOperator != null &&
                                    !(record.RetriveData(neighbor.Item1 + (neighbor.Item3 ? ADJ_OFFSET : REV_ADJ_OFFSET)).Contains(ID)))
                                        ValidFlag = false;


                                }
                            if (ValidFlag)
                            {
                                // If aligned and reverse checked vailied, join the old record with the new one.
                                RawRecord NewRecord = ConstructRawRecord(NumberOfProcessedVertices, ItemInfo, record,
                                    header, true);
                                // Deduplication.
                                if (RecordFilter(crossDocumentJoinPredicates,NewRecord))
                                    if (!UniqueRecord.Contains(NewRecord.RetriveData(NumberOfProcessedVertices  * 3) + NewRecord.RetriveData(src)))
                                    {
                                        OutputBuffer.Enqueue(NewRecord);
                                        UniqueRecord.Add(NewRecord.RetriveData(NumberOfProcessedVertices * 3) + NewRecord.RetriveData(src));
                                    }
                            }
                        }
                    }
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                } 
            }
            InputBuffer.Clear();
            if (OutputBuffer.Count <= 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        private Queue<Tuple<RawRecord, string>> PathFunction(RawRecord InputRecord)
        {
            Queue<Tuple<RawRecord, string>> SaveQueue = new Queue<Tuple<RawRecord, string>>();
            Queue<Tuple<RawRecord, string>> WorkQueue = new Queue<Tuple<RawRecord, string>>();
            Queue<Tuple<RawRecord, string>> TempQueue = new Queue<Tuple<RawRecord, string>>();

            int StartNodeIndex = 0;

            TraversalOperator root = InternalOperator as TraversalOperator;
            while (!(root.ChildOperator is FetchNodeOperator || root.ChildOperator is ConstantSourceOperator))
                root = root.ChildOperator as TraversalOperator;
            ConstantSourceOperator source = new ConstantSourceOperator(InputRecord);
            root.ChildOperator = source;

            TempQueue.Enqueue(new Tuple<RawRecord, string>(InputRecord, ""));

            while (TempQueue.Count != 0)
            {
                SaveQueue.Clear();
                foreach (var x in TempQueue)
                {
                    WorkQueue.Enqueue(x);
                    SaveQueue.Enqueue(x);
                }
                TempQueue.Clear();
                while (WorkQueue.Count != 0)
                {
                    var start = WorkQueue.Dequeue();
                    var SourceRecord = new RawRecord(0);
                    SourceRecord.fieldValues.Add(start.Item1.fieldValues[StartNodeIndex * 3]);
                    SourceRecord.fieldValues.Add(start.Item1.fieldValues[StartNodeIndex * 3 + 1]);
                    SourceRecord.fieldValues.Add(start.Item1.fieldValues[StartNodeIndex * 3 + 2]);
                    SourceRecord.fieldValues.Add(start.Item1.fieldValues[start.Item1.fieldValues.Count - 1]);
                    source.ConstantSource = SourceRecord;
                    (InternalOperator as TraversalOperator).ResetState();
                    StartNodeIndex = InternalOperator.NumberOfProcessedVertices;
                    while (InternalOperator.State())
                    {
                        var EndRecord = InternalOperator.Next();
                        var sink = EndRecord.fieldValues[StartNodeIndex * 3 + 1];
                        if (sink != "")
                        TempQueue.Enqueue(new Tuple<RawRecord, string>(EndRecord, sink));
                    }
                }
            }
            return SaveQueue;
        }

        private void ResetState()
        {
            GraphViewExecutionOperator RootOperator = this;
            while (RootOperator is TraversalOperator)
                    {
                RootOperator.Open();
              RootOperator = (RootOperator as TraversalOperator).ChildOperator;
              }
              RootOperator.Open();
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


        // For case N1 -> N2 -> N3
        //                 ↑
        //                N4
        // If using topological sorting, after finishing N1 -> N2 -> N3, a FetchNode operator will be needed for N4,
        // so it takes the records from previous operator, find N4 and join them together.
        private GraphViewExecutionOperator ChildOperator;

        private string docDbScript;

        public FetchNodeOperator(GraphViewConnection pConnection, string pScript, int pnode, List<string> pheader, int pNumberOfProcessedVertices, int pOutputBufferSize, GraphViewExecutionOperator pChildOperator = null)
        {
            this.Open();
            connection = pConnection;
            docDbScript = pScript;
            header = pheader;
            NumberOfProcessedVertices = pNumberOfProcessedVertices;
            ChildOperator = pChildOperator;
        }
        override public RawRecord Next()
        {
            // Set up output buffer
            if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1)
                    if (ChildOperator == null || !ChildOperator.State()) this.Close();
                return OutputBuffer.Dequeue();
            }
            string script = docDbScript;
            RawRecord OldRecord = new RawRecord(header.Count);

            if (ChildOperator != null && ChildOperator.State())
                OldRecord = ChildOperator.Next();
            try
            {   // Send query to the server
                IQueryable<dynamic> Node = (IQueryable<dynamic>) SendQuery(script, connection);
                // Decode the result retrived from server and generate new record
                foreach (var item in Node)
                {
                    Tuple<string, string, string, List<string>> ItemInfo = DecodeJObject((JObject) item,header, NumberOfProcessedVertices);
                    RawRecord NewRecord = ConstructRawRecord(NumberOfProcessedVertices,ItemInfo, OldRecord, header,true);
                        if (RecordFilter(crossDocumentJoinPredicates,NewRecord))
                        OutputBuffer.Enqueue(NewRecord);
                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
            // Close output buffer
            if (OutputBuffer.Count <= 1 && (ChildOperator ==null || !ChildOperator.State())) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
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
                for (int i = 0; i < header.Count; i++)
                {
                    if (NewResult.fieldValues[i] == "" && record.fieldValues[i] != "")
                        NewResult.fieldValues[i] = record.fieldValues[i];
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
        internal Order order;

        public enum Order
        {
            Decr,
            Incr,
            NotSpecified
        }
        public OrderbyOperator(GraphViewExecutionOperator pChildOperator, string pBywhat, List<string> pheader, Order pOrder = Order.NotSpecified)
        {
            this.Open();
            header = pheader;
            ChildOperator = pChildOperator;
            bywhat = pBywhat;
            order = pOrder;
        }

        override public RawRecord Next()
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
                if (order == Order.Incr || order == Order.NotSpecified)
                    results.Sort((x, y) => string.Compare(x.RetriveData(header, bywhat), y.RetriveData(header, bywhat), StringComparison.OrdinalIgnoreCase));
                if (order == Order.Decr)
                    results.Sort((x, y) => string.Compare(y.RetriveData(header, bywhat), x.RetriveData(header, bywhat), StringComparison.OrdinalIgnoreCase));

                ResultQueue = new Queue<RawRecord>();
                foreach (var x in results)
                    ResultQueue.Enqueue(x);
            }
            if (ResultQueue.Count <= 1) this.Close();
            return ResultQueue.Dequeue();
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

