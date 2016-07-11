using System;
using System.Linq;
using System.Collections.Generic;
// Add DocumentDB references
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;

namespace GraphView
{
    /// <summary>
    /// TraversalOperator is used to traval a graph pattern and return asked result.
    /// TraversalOperator.Next() returns one result of what its specifier specified.
    /// By connecting TraversalProcessor together it returns the final result.
    /// </summary>
    public class TraversalOperator : GraphViewOperator
    {
        // Buffer on both input and output sides.
        private Queue<Record> InputBuffer;
        private Queue<Record> OutputBuffer;
        private int InputBufferSize;
        private int OutputBufferSize;
        // The operator that gives the current operator input.
        private GraphViewOperator ChildOperator;

        // Addition info to interpret the output record of traversal operator
        private int StartOfResultField;

        // GraphView connection.
        private GraphViewConnection connection;

        // Defining from which field in the record does the traversal goes to which field.
        private int src;
        private int dest;

        // Segement of DocDb script for querying.
        private string docDbScript;

        // Defining which fields should the reverse check have on.
        private Dictionary<int,string> ReverseCheckList;

        public TraversalOperator(GraphViewConnection pConnection, GraphViewOperator pChildProcessor, string pScript, int pSrc, int pDest, List<string> pheader, Dictionary<int, string> pReverseCheckList, int pStartOfResultField, int pInputBufferSize, int pOutputBufferSize)
        {
            this.Open();
            ChildOperator = pChildProcessor;
            connection = pConnection;
            docDbScript = pScript;
            InputBufferSize = pInputBufferSize;
            OutputBufferSize = pOutputBufferSize;
            InputBuffer = new Queue<Record>();
            InputBuffer = new Queue<Record>();
            src = pSrc;
            dest = pDest;
            ReverseCheckList = pReverseCheckList;
            header = pheader;
            StartOfResultField = pStartOfResultField;
        }
        override public Record Next()
        {
            // If the output buffer is not empty, return a result.
            if (OutputBuffer == null)
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (ChildOperator != null && !ChildOperator.Status())))
            {
                return OutputBuffer.Dequeue();
            }

            // Take input from the output buffer of its child operator.

            while (InputBuffer.Count() < InputBufferSize && ChildOperator.Status())
            {
                if (ChildOperator != null && ChildOperator.Status())
                {
                    Record Result = (Record)ChildOperator.Next();
                    if (Result == null) ChildOperator.Close();
                    else
                        InputBuffer.Enqueue(Result);
                }
            }

            // Consturct the "IN" clause
            string InRangeScript = "";
            foreach (Record record in InputBuffer)
            {
                if (record.RetriveData(src + 1) != "") InRangeScript += record.RetriveData(src + 1) + ",";
            }
            InRangeScript = CutTheTail(InRangeScript);
            if (InputBuffer.Count != 0)
            {
                string script = docDbScript;
                if (InRangeScript != "")
                    if (!script.Contains("WHERE"))
                        script += "WHERE " + header[dest] + ".id IN (" + InRangeScript + ")";
                else script += " AND " + header[dest] + ".id IN (" + InRangeScript + ")";

                // Send query to server and decode the result.
                IQueryable<dynamic> Node = (IQueryable<dynamic>)SendQuery(script, connection);
                HashSet<Tuple<string, string, string>> UniqueRecord = new HashSet<Tuple<string, string, string>>();
                foreach (var item in Node)
                {
                    Tuple<string, string, string> ItemInfo = DecodeJObject((JObject)item);
                    string ID = ItemInfo.Item1;
                    string edges = ItemInfo.Item2;
                    string ReverseEdge = ItemInfo.Item3;
                    if (!UniqueRecord.Contains(ItemInfo))
                    {
                        UniqueRecord.Add(ItemInfo);
                        Record ResultRecord = new Record(header.Count());
                        foreach (string ResultFieldName in header.GetRange(StartOfResultField, header.Count - StartOfResultField))
                        {
                            string result = "";
                            if (((JObject)item)[ResultFieldName.Replace(".", "_")] != null)
                                result = ((JObject)item)[ResultFieldName.Replace(".", "_")].ToString();
                            ResultRecord.field[header.IndexOf(ResultFieldName)] = result;

                        }
                        // Do reverse check, and put vailed result into output buffer
                        foreach (var record in InputBuffer)
                        {
                            // reverse check
                            foreach (var ReverseNode in ReverseCheckList)
                            {
                                string Edge = (((JObject)item)[ReverseNode.Value])["_sink"].ToString();
                                if ((Edge == record.RetriveData(ReverseNode.Key)) && record.RetriveData(ReverseNode.Key + 1).Contains(ID))
                                {
                                    Record NewRecord = AddIfNotExist(ItemInfo, record, ResultRecord.field, header);
                                    OutputBuffer.Enqueue(NewRecord);
                                }
                            }
                        }
                    }
                }
                InputBuffer.Clear();
            }
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }
            return null;
        }

        // Send a query to server and retrive result.
        private IQueryable<dynamic> SendQuery(string script, GraphViewConnection connection)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DocDB_DatabaseId, connection.DocDB_CollectionId), script, QueryOptions);
            return Result;
        }

        private bool HasWhereClause(string SelectClause)
        {
            return !(SelectClause.Length < 6 || SelectClause.Substring(SelectClause.Length - 6, 5) == "WHERE");
        }
        /// <summary>
        /// Break down a JObject that return by server and extract the id and edge infomation from it.
        /// </summary>
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
        private Record AddIfNotExist(Tuple<string, string, string> ItemInfo, Record record, List<string> Result, List<string> header)
        {
            Record NewRecord = new Record(record);
            if (NewRecord.RetriveData(dest) == "") NewRecord.field[dest] = ItemInfo.Item1;
            if (NewRecord.RetriveData(dest + 1) == "") NewRecord.field[dest + 1] = ItemInfo.Item2;
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
    public class FetchNodeOperator : GraphViewOperator
    {
        internal static Record RecordZero;
        private Queue<Record> OutputBuffer;
        private int OutputBufferSize;

        private int StartOfResultField;

        private GraphViewConnection connection;

        private int node;

        private string docDbScript;

        public FetchNodeOperator(GraphViewConnection pConnection, string pScript, int pnode, List<string> pheader, int pStartOfResultField, int pOutputBufferSize)
        {
            this.Open();
            connection = pConnection;
            docDbScript = pScript;
            OutputBufferSize = pOutputBufferSize;
            node = pnode;
            header = pheader;
            StartOfResultField = pStartOfResultField;
            if (RecordZero == null) RecordZero = new Record(pheader.Count);
        }
        override public Record Next()
        {
            // Set up output buffer
            if (OutputBuffer == null)
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count == 1) this.Close();
            if (OutputBuffer.Count != 0)
            {
                return OutputBuffer.Dequeue();
            }
            string script = docDbScript;
            // Send query to the server
            IQueryable<dynamic> Node = (IQueryable<dynamic>)SendQuery(script, connection);
            HashSet<Tuple<string, string, string>> UniqueRecord = new HashSet<Tuple<string, string, string>>();
            // Decode the result retrived from server and generate new record.
            foreach (var item in Node)
            {
                Tuple<string, string, string> ItemInfo = DecodeJObject((JObject)item);
                string ID = ItemInfo.Item1;
                string edges = ItemInfo.Item2;

                if (!UniqueRecord.Contains(ItemInfo))
                {
                    UniqueRecord.Add(ItemInfo);
                    Record ResultRecord = new Record(header.Count());

                    foreach (string ResultFieldName in header.GetRange(StartOfResultField, header.Count - StartOfResultField))
                    {
                        string result = "";
                        if (((JObject)item)[ResultFieldName.Replace(".", "_")] != null)
                            result = ((JObject)item)[ResultFieldName.Replace(".", "_")].ToString();
                        ResultRecord.field[header.IndexOf(ResultFieldName)] = result;
                    }
                    Record NewRecord = AddIfNotExist(ItemInfo, RecordZero, ResultRecord.field, header);
                    OutputBuffer.Enqueue(NewRecord);
                }
            }
            // Close output buffer
            if (OutputBuffer.Count == 1) this.Close();
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

        private Record AddIfNotExist(Tuple<string, string, string> ItemInfo, Record record, List<string> Result, List<string> header)
        {
            Record NewRecord = new Record(record);
            if (NewRecord.RetriveData(node) == "") NewRecord.field[node] = ItemInfo.Item1;
            if (NewRecord.RetriveData(node + 1) == "") NewRecord.field[node + 1] = ItemInfo.Item2;
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
    public class CartesianProductOperator : GraphViewOperator
    {
        private List<GraphViewOperator> OperatorOnSubGraphs;

        private Queue<Record> OutputBuffer;
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

        override public Record Next()
        {
            if (OutputBuffer == null)
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count != 0)
            {
                return OutputBuffer.Dequeue();
            }

            List<List<Record>> ResultsFromChildrenOperator = new List<List<Record>>();

            foreach (var ChildOperator in OperatorOnSubGraphs)
            {
                ResultsFromChildrenOperator.Add(new List<Record>());
                Record result = ChildOperator.Next();
                while (result != null && ChildOperator.Status())
                {
                    ResultsFromChildrenOperator.Last().Add(result);
                    result = ChildOperator.Next();
                }
                if (result != null) ResultsFromChildrenOperator.Last().Add(result);
            }
            CartesianProductOnRecord(ResultsFromChildrenOperator, 0, new Record(header.Count));

            if (OutputBuffer.Count == 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        // Find the cartesian product of giving sets of records.
        private void CartesianProductOnRecord(List<List<Record>> RecordSet, int IndexOfOperator, Record result)
        {
            if (IndexOfOperator == RecordSet.Count)
            {
                OutputBuffer.Enqueue(result);
                return;
            }
            foreach (var record in RecordSet[IndexOfOperator])
            {
                Record NewResult = new Record(result);
                for (int i = 0; i < header.Count; i++)
                {
                    if (NewResult.field[i] == "" && record.field[i] != "")
                        NewResult.field[i] = record.field[i];
                }
                CartesianProductOnRecord(RecordSet, IndexOfOperator + 1, NewResult);
            }
        }
    }
}

