using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Remoting.Messaging;
using System.Security.Policy;
// Add DocumentDB references
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// TraversalOperator is used to traval a graph pattern and return asked result.
    /// TraversalOperator.Next() returns one result of what its specifier specified.
    /// By connecting TraversalProcessor together it returns the final result.
    /// </summary>
    internal class TraversalOperator : GraphViewOperator
    {
        // Buffer on both input and output sides.
        private Queue<RawRecord> InputBuffer;
        private Queue<RawRecord> OutputBuffer;
        private int InputBufferSize;
        private int OutputBufferSize;
        // The operator that gives the current operator input.
        internal GraphViewOperator ChildOperator;

        // Addition info to interpret the output record of traversal operator
        private int StartOfResultField;

        // GraphView connection.
        private GraphViewConnection connection;

        // Defining from which field in the record does the traversal goes to which field.
        internal int src;
        internal int dest;

        // Segement of DocDb script for querying.
        private string docDbScript;

        // Defining which fields should the reverse check have on.
        private List<Tuple<int,string,bool>> ReverseCheckList;
        internal BooleanFunction BooleanCheck;

        internal GraphViewOperator InternalOperator;
        List<string> InternalHeader;
        private int InternalLoopStartNode;

        private bool reverse; 

        internal TraversalOperator(GraphViewConnection pConnection, GraphViewOperator pChildProcessor, string pScript, int pSrc, int pDest, List<string> pheader, List<Tuple<int, string, bool>> pReverseCheckList, int pStartOfResultField, int pInputBufferSize, int pOutputBufferSize, bool pReverse, GraphViewOperator pInternalOperator = null, BooleanFunction pBooleanCheck = null)
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
            dest = pDest;
            ReverseCheckList = pReverseCheckList;
            header = pheader;
            StartOfResultField = pStartOfResultField;
            BooleanCheck = pBooleanCheck;
            InternalOperator = pInternalOperator;
            reverse = pReverse;
            if (InternalOperator != null) InternalOperator = (InternalOperator as OutputOperator).ChildOperator;
        }
        override public RawRecord Next()
        {
            // If the output buffer is not empty, return a result.
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (ChildOperator != null && !ChildOperator.Status())))
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }

            // Take input from the output buffer of its child operator.
            while (InputBuffer.Count() < InputBufferSize && ChildOperator.Status())
            {
                if (ChildOperator != null && ChildOperator.Status())
                {
                    RawRecord Result = (RawRecord)ChildOperator.Next();
                    if (Result == null) ChildOperator.Close();
                    else if (InternalOperator != null)
                        PathFunction(Result, ref InputBuffer);
                    else InputBuffer.Enqueue(Result);
                }
            }

            // Consturct the "IN" clause
            if (InputBuffer.Count != 0)
            {
                string InRangeScript = "";
                HashSet<string> RefSet = new HashSet<string>();
                foreach (RawRecord record in InputBuffer)
                {
                    var adj = record.RetriveData(src + (reverse?2:1)).Split(',');
                    foreach (var x in adj)
                    {
                        if (!RefSet.Contains(x))
                            InRangeScript += x + ",";
                        RefSet.Add(x);
                    }
                    //if (record.RetriveData(src + 1) != "") InRangeScript += record.RetriveData(src + 1) + ",";
                }
                InRangeScript = CutTheTail(InRangeScript);
                string script = docDbScript;
                if (InRangeScript != "")
                    if (!script.Contains("WHERE"))
                        script += "WHERE " + header[dest] + ".id IN (" + InRangeScript + ")";
                    else script += " AND " + header[dest] + ".id IN (" + InRangeScript + ")";
                HashSet<string> UniqueRecord = new HashSet<string>();

                // Send query to server and decode the result.
                try
                {
                    IQueryable<dynamic> Node = (IQueryable<dynamic>) SendQuery(script, connection);
                    foreach (var item in Node)
                    {
                        // Decode some information that describe the found node.
                        Tuple<string, string, string> ItemInfo = DecodeJObject((JObject) item);
                        string ID = ItemInfo.Item1;

                        // Generate the result list that need to be union with the original one
                        RawRecord ResultRecord = new RawRecord(header.Count());
                        foreach (
                            string ResultFieldName in
                                header.GetRange(StartOfResultField, header.Count - StartOfResultField))
                        {
                            string result = "";
                            if (((JObject) item)[ResultFieldName.Replace(".", "_")] != null)
                                result = ((JObject) item)[ResultFieldName.Replace(".", "_")].ToString();
                            ResultRecord.field[header.IndexOf(ResultFieldName)] = result;
                        }
                        // Join the old record with the new one if checked vailed.
                        foreach (var record in InputBuffer)
                        {
                            // reverse check
                            bool VailedFlag = true;
                            if (ReverseCheckList != null)
                                foreach (var ReverseNode in ReverseCheckList)
                                {
                                    string Edge = (((JObject) item)[ReverseNode.Item2])["_sink"].ToString();
                                    if (
                                        !(Edge == record.RetriveData(ReverseNode.Item1) &&
                                          record.RetriveData(ReverseNode.Item1 + (ReverseNode.Item3 ?1:2)).Contains(ID)) &&
                                        InternalOperator == null)
                                        VailedFlag = false;
                                    if (!(record.RetriveData(ReverseNode.Item1 + (ReverseNode.Item3? 1 : 2)).Contains(ID)) &&
                                        InternalOperator != null)
                                        VailedFlag = false;
                                }
                            if (VailedFlag)
                            {
                                RawRecord NewRecord = InternalOperator == null? AddIfNotExist(ItemInfo, record, ResultRecord.field, header,true): AddIfNotExist(ItemInfo, record, ResultRecord.field, header, false);
                                if (RecordFilter(NewRecord))
                                    if (!UniqueRecord.Contains(NewRecord.RetriveData(dest) + NewRecord.RetriveData(src)))
                                    {
                                        OutputBuffer.Enqueue(NewRecord);
                                        UniqueRecord.Add(NewRecord.RetriveData(dest) + NewRecord.RetriveData(src));
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

        private void PathFunction(RawRecord input, ref Queue<RawRecord> ResultQueue)
        {
            EdgeRefOperator EdgeRef = new EdgeRefOperator(null);
            TraversalOperator Tra = InternalOperator as TraversalOperator;
            while (!(Tra.ChildOperator is FetchNodeOperator || Tra.ChildOperator is EdgeRefOperator)) Tra = Tra.ChildOperator as TraversalOperator;
            if (Tra.ChildOperator is FetchNodeOperator)
                InternalLoopStartNode = (Tra.ChildOperator as FetchNodeOperator).node;
            Tra.ChildOperator = EdgeRef;
            InternalHeader = Tra.header;
            RawRecord ConstantRecord = new RawRecord(InternalOperator.header.Count);
            if (ChildOperator is FetchNodeOperator)
            {
                ConstantRecord.field[InternalLoopStartNode] = input.field[(ChildOperator as FetchNodeOperator).node];
                ConstantRecord.field[InternalLoopStartNode + 1] = input.field[(ChildOperator as FetchNodeOperator).node + 1];
                ConstantRecord.field[ConstantRecord.field.Count - 1] = input.field[input.field.Count - 1];
            }
            if (ChildOperator is TraversalOperator)
            {
                ConstantRecord.field[InternalLoopStartNode] = input.field[(ChildOperator as TraversalOperator).dest];
                ConstantRecord.field[InternalLoopStartNode + 1] = input.field[(ChildOperator as TraversalOperator).dest + 1];
                ConstantRecord.field[ConstantRecord.field.Count - 1] = input.field[input.field.Count - 1];
            }
            RecursivePathTraversal(input, ConstantRecord, ref EdgeRef, InternalLoopStartNode, ref ResultQueue);
        }
        public void RecursivePathTraversal(RawRecord OriginalInput, RawRecord input, ref EdgeRefOperator source, int StartNode, ref Queue<RawRecord> ResultQueue)
        {
            Queue<RawRecord> StageRecords = new Queue<RawRecord>(); ;
            bool LoopEnd = true;
            if (input.field[StartNode + 1] != "")
            {
                source.SetRef(input);
                GraphViewOperator RootOperator = InternalOperator;
                while (RootOperator is TraversalOperator)
                {
                    RootOperator.Open();
                    RootOperator = (RootOperator as TraversalOperator).ChildOperator;
                }
                RootOperator.Open();
                while (InternalOperator.Status() || StageRecords.Count > 0)
                {
                    while (InternalOperator.Status()) StageRecords.Enqueue(InternalOperator.Next());
                    RawRecord OldOutput = StageRecords.Dequeue();
                    if (OldOutput != null)
                    {
                        RawRecord NewInput = new RawRecord(InternalHeader.Count);
                        NewInput.field[StartNode] = OldOutput.field[(InternalOperator as TraversalOperator).dest];
                        NewInput.field[StartNode + 1] =
                            OldOutput.field[(InternalOperator as TraversalOperator).dest + 1];
                        NewInput.field[NewInput.field.Count - 1] = OldOutput.field[OldOutput.field.Count - 1];
                        RecursivePathTraversal(OriginalInput, NewInput, ref source, StartNode, ref ResultQueue);
                        LoopEnd = false;
                    }
                }
            }
            if (LoopEnd == true)
            {
                RawRecord DestRecord = new RawRecord(OriginalInput);
                DestRecord.field[src + 1] = "\"" + input.field[StartNode] + "\"";
                DestRecord.field[DestRecord.field.Count - 1] = input.field[input.field.Count - 1];
                ResultQueue.Enqueue(DestRecord);
            }
        }
        // Send a query to server and retrive result.
        private IQueryable<dynamic> SendQuery(string script, GraphViewConnection connection)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DocDB_DatabaseId, connection.DocDB_CollectionId), script, QueryOptions);
            return Result;
        }

        private bool RecordFilter(RawRecord r)
        {
            if (BooleanCheck == null) return true;
            else return BooleanCheck.eval(r);
        }
        private Tuple<string, string, string> DecodeJObject(JObject Item, bool ShowEdge = false)
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
            return new Tuple<string, string, string>(id.ToString(), CutTheTail(EdgeID), CutTheTail(ReverseEdgeID));
        }
        // Combine two records
        private RawRecord AddIfNotExist(Tuple<string, string, string> ItemInfo, RawRecord record, List<string> Result, List<string> header, bool addpath)
        {
            RawRecord NewRecord = new RawRecord(record);
            //if (NewRecord.RetriveData(dest) == "")
                NewRecord.field[dest] = ItemInfo.Item1;
            //if (NewRecord.RetriveData(dest + 1) == "")
                NewRecord.field[dest + 1] = ItemInfo.Item2;
            //if (NewRecord.RetriveData(dest + 2) == "")
                NewRecord.field[dest + 2] = ItemInfo.Item3;
            if (addpath) NewRecord.field[NewRecord.field.Count - 1] += ItemInfo.Item1 + ",";
            for (int i = 0; i < NewRecord.field.Count; i++)
            {
                if (NewRecord.RetriveData(i) == "" && Result[i] != "")
                    NewRecord.field[i] = Result[i];
            }
            return NewRecord;
        }
        string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
    }

    /// <summary>
    /// FetchNodeOperator is used to fetch a certain node.
    /// FetchNodeOperator.Next() returns one result of what its specifier specified.
    /// It often used as the input of a traversal operator
    /// </summary>
    internal class FetchNodeOperator : GraphViewOperator
    {
        private RawRecord RecordZero;
        private Queue<RawRecord> OutputBuffer;
        private int OutputBufferSize;

        private int StartOfResultField;

        private GraphViewConnection connection;

        private GraphViewOperator ChildOperator;
        internal int node;

        private string docDbScript;

        public FetchNodeOperator(GraphViewConnection pConnection, string pScript, int pnode, List<string> pheader, int pStartOfResultField, int pOutputBufferSize, GraphViewOperator pChildOperator = null)
        {
            this.Open();
            connection = pConnection;
            docDbScript = pScript;
            OutputBufferSize = pOutputBufferSize;
            node = pnode;
            header = pheader;
            StartOfResultField = pStartOfResultField;
            RecordZero = new RawRecord(pheader.Count);
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
                    if (ChildOperator == null || !ChildOperator.Status()) this.Close();
                return OutputBuffer.Dequeue();
            }
            string script = docDbScript;
            if (ChildOperator != null && ChildOperator.Status())
                RecordZero = ChildOperator.Next();
            // Send query to the server
            try
            {
                IQueryable<dynamic> Node = (IQueryable<dynamic>) SendQuery(script, connection);
                // Decode the result retrived from server and generate new record
                HashSet<string> UniqueRecord = new HashSet<string>();
                foreach (var item in Node)
                {
                    Tuple<string, string, string> ItemInfo = DecodeJObject((JObject) item);
                        RawRecord ResultRecord = new RawRecord(header.Count());

                        foreach (
                            string ResultFieldName in
                                header.GetRange(StartOfResultField, header.Count - StartOfResultField))
                        {
                            string result = "";
                            if (((JObject) item)[ResultFieldName.Replace(".", "_")] != null)
                                result = ((JObject) item)[ResultFieldName.Replace(".", "_")].ToString();
                            ResultRecord.field[header.IndexOf(ResultFieldName)] = result;
                        }
                        RawRecord NewRecord = AddIfNotExist(ItemInfo, RecordZero, ResultRecord.field, header);
                        OutputBuffer.Enqueue(NewRecord);

                }
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
            // Close output buffer
            if (OutputBuffer.Count <= 1 && (ChildOperator ==null || !ChildOperator.Status())) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        private IQueryable<dynamic> SendQuery(string script, GraphViewConnection connection)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DocDB_DatabaseId, connection.DocDB_CollectionId), script, QueryOptions);
            return Result;
        }

        private Tuple<string, string, string> DecodeJObject(JObject Item, bool ShowEdge = false)
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
            return new Tuple<string, string, string>(id.ToString(), CutTheTail(EdgeID), CutTheTail(ReverseEdgeID));
        }

        private RawRecord AddIfNotExist(Tuple<string, string, string> ItemInfo, RawRecord record, List<string> Result, List<string> header)
        {
            RawRecord NewRecord = new RawRecord(record);
            if (NewRecord.RetriveData(node) == "") NewRecord.field[node] = ItemInfo.Item1;
            if (NewRecord.RetriveData(node + 1) == "") NewRecord.field[node + 1] = ItemInfo.Item2;
            if (NewRecord.RetriveData(node + 2) == "") NewRecord.field[node + 2] = ItemInfo.Item3;
            NewRecord.field[NewRecord.field.Count - 1] += ItemInfo.Item1 + ",";
            for (int i = 0; i < NewRecord.field.Count; i++)
            {
                if (NewRecord.RetriveData(i) == "" && Result[i] != "")
                    NewRecord.field[i] = Result[i];
            }
            return NewRecord;
        }

        string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
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
    internal class CartesianProductOperator : GraphViewOperator
    {
        private List<GraphViewOperator> OperatorOnSubGraphs;

        private Queue<RawRecord> OutputBuffer;
        private int OutputBufferSize;

        private GraphViewConnection connection;
        public CartesianProductOperator(GraphViewConnection pConnection, List<GraphViewOperator> pProcessorOnSubGraph, List<string> pheader, int pOutputBufferSize)
        {
            this.Open();
            connection = pConnection;
            OutputBufferSize = pOutputBufferSize;
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

            foreach (var ChildOperator in OperatorOnSubGraphs)
            {
                ResultsFromChildrenOperator.Add(new List<RawRecord>());
                RawRecord result = ChildOperator.Next();
                while (result != null && ChildOperator.Status())
                {
                    ResultsFromChildrenOperator.Last().Add(result);
                    result = ChildOperator.Next();
                }
                if (result != null) ResultsFromChildrenOperator.Last().Add(result);
            }
            if (OperatorOnSubGraphs.Count != 0) CartesianProductOnRecord(ResultsFromChildrenOperator, 0, new RawRecord(header.Count));
            OperatorOnSubGraphs.Clear();
            if (OutputBuffer.Count < 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
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
                    if (NewResult.field[i] == "" && record.field[i] != "")
                        NewResult.field[i] = record.field[i];
                }
                CartesianProductOnRecord(RecordSet, IndexOfOperator + 1, NewResult);
            }
        }
    }

    internal class UnionOperator : GraphViewOperator
    {
        internal List<GraphViewOperator> Sources;
        internal GraphViewConnection connection;
        internal int FromWhichSource;
        internal RawRecord result;
        public UnionOperator(GraphViewConnection pConnection, List<GraphViewOperator> pSources)
        {
            this.Open();
            connection = pConnection;
            Sources = pSources;
            FromWhichSource = 0;
        }

        override public RawRecord Next()
        {
            if (!Sources[FromWhichSource].Status() || (result = Sources[FromWhichSource].Next()) == null)
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

    internal class CoalesceOperator : GraphViewOperator
    {
        internal List<GraphViewOperator> Sources;
        internal GraphViewConnection connection;
        internal int FromWhichSource;
        internal RawRecord result;
        internal int CoalesceNumber;
        public CoalesceOperator(GraphViewConnection pConnection, List<GraphViewOperator> pSources, int pCoalesceNumber)
        {
            this.Open();
            connection = pConnection;
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
            while (FromWhichSource < Sources.Count && (!Sources[FromWhichSource].Status() || (result = Sources[FromWhichSource].Next()) == null))
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

    internal class OrderbyOperator : GraphViewOperator
    {
        internal GraphViewOperator ChildOperator;
        internal GraphViewConnection connection;
        internal List<RawRecord> results;
        internal Queue<RawRecord> ResultQueue;
        internal string bywhat;
        internal Order order;

        public enum Order
        {
            Decr,
            Incr,
            NotSpecified
        }
        public OrderbyOperator(GraphViewConnection pConnection, GraphViewOperator pChildOperator, string pBywhat, List<string> pheader, Order pOrder = Order.NotSpecified)
        {
            this.Open();
            connection = pConnection;
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
                while (ChildOperator.Status())
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

    internal class OutputOperator : GraphViewOperator
    {
        internal GraphViewOperator ChildOperator;
        internal GraphViewConnection connection;
        internal List<string> SelectedElement;
        internal bool OutputPath;

        public OutputOperator(GraphViewOperator pChildOperator, GraphViewConnection pConnection, List<string> pSelectedElement, List<string> pHeader)
        {
            this.Open();
            ChildOperator = pChildOperator;
            connection = pConnection;
            SelectedElement = pSelectedElement;
            header = pHeader;
        }

        public OutputOperator(GraphViewOperator pChildOperator, GraphViewConnection pConnection,
            bool pOutputPath, List<string> pHeader)
        {
            this.Open();
            ChildOperator = pChildOperator;
            connection = pConnection;
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
                if (ChildOperator.Status())
                {
                    while ((InputRecord = ChildOperator.Next()) == null && ChildOperator.Status()) ;
                    if (!ChildOperator.Status())
                    {
                        this.Close();
                    }
                    if (InputRecord != null)
                    {
                        OutputRecord.field[0] = CutTheTail(InputRecord.field.Last());
                        return OutputRecord;
                    }
                    else return null;
                }
            }
            else if (SelectedElement != null)
            {
                RawRecord OutputRecord = new RawRecord(SelectedElement.Count);
                RawRecord InputRecord = null;
                if (ChildOperator.Status())
                {
                    while ((InputRecord = ChildOperator.Next()) == null && ChildOperator.Status()) ;
                    if (!ChildOperator.Status())
                    {
                        this.Close();
                    }
                    if (InputRecord != null)
                    {
                        foreach (var x in SelectedElement)
                            OutputRecord.field[SelectedElement.IndexOf(x)] = InputRecord.RetriveData(ChildOperator.header, x);
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

        string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
    }

    internal class EdgeRefOperator : GraphViewOperator
    {
        internal RawRecord EdgeRef;

        public EdgeRefOperator(RawRecord pConstant)
        {
            EdgeRef = pConstant;
            this.Open();
        }

        public void SetRef(RawRecord pConstant)
        {
            EdgeRef = pConstant;
            this.Open();
        }

        override public RawRecord Next()
        {
            this.Close();
            return EdgeRef;
        }

    }

    public class GraphViewDataReader : IDataReader
    {
        private GraphViewOperator DataSource;
        RawRecord CurrentRecord;
        internal GraphViewDataReader(GraphViewOperator pDataSource)
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
                return CurrentRecord.RetriveData(DataSource.header, FieldName);
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

