using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;
// Add DocumentDB references
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;

namespace GraphView
{
    /// <summary>
    /// TraversalProcessor is used to traval a graph pattern and return asked result.
    /// TraversalProcessor.Next() returns one result of what its specifier specified.
    /// By connecting TraversalProcessor together it returns the final result.
    /// </summary>
    internal class TraversalOperator : GraphViewOperator
    {
        internal static Record RecordZero;
        private Queue<Record> InputBuffer;
        private Queue<Record> OutputBuffer;
        private int InputBufferSize;
        private int OutputBufferSize;
        private GraphViewOperator ChildOperator;

        private int StartOfResultField;

        private List<string> header;
        private GraphViewConnection connection;

        private int src;
        private int dest;

        private string docDbScript;

        private List<int> ReverseCheckList;

        public TraversalOperator(GraphViewConnection pConnection, GraphViewOperator pChildProcessor, string pScript, int pSrc, int pDest, List<string> pheader, List<int> pReverseCheckList, int pStartOfResultField, int pInputBufferSize, int pOutputBufferSize)
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
            if (RecordZero == null) RecordZero = new Record(pheader.Count);
        }
        override public Record Next()
        {
            if (OutputBuffer == null)
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (ChildOperator != null && !ChildOperator.Status())))
            {
                return OutputBuffer.Dequeue();
            }

            if (ChildOperator == null && this.Status())
            {
                if (OutputBuffer.Count == 0) InputBuffer.Enqueue(RecordZero);
            }
            else
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
            string InRangeScript = "";
            foreach (Record record in InputBuffer)
            {
                if (record.RetriveData(src + 1) != "") InRangeScript += record.RetriveData(src + 1) + ",";
            }
            InRangeScript = CutTheTail(InRangeScript);
            if (InputBuffer.Count != 0)
            {
                string script = docDbScript;
                if (src != -1 && InRangeScript != "") script += " AND " + header[dest] + ".id IN (" + InRangeScript + ")";
                IQueryable<dynamic> Node = (IQueryable<dynamic>)FectNode(script, connection);
                foreach (var item in Node)
                {
                    Tuple<string, string, string> ItemInfo = DecodeJObject((JObject)item);
                    string ID = ItemInfo.Item1;
                    string edges = ItemInfo.Item2;
                    string ReverseEdge = ItemInfo.Item3;
                    Record ResultRecord = new Record(header.Count());
                    foreach (string ResultFieldName in header.GetRange(StartOfResultField, header.Count - StartOfResultField))
                    {
                        string result = "";
                        if (((JObject)item)[ResultFieldName.Replace(".", "_")] != null)
                            result = ((JObject)item)[ResultFieldName.Replace(".", "_")].ToString();
                        ResultRecord.field[header.IndexOf(ResultFieldName)] = result;

                    }
                    foreach (var record in InputBuffer)
                    {
                        if (src == -1)
                        {
                            Record NewRecord = AddIfNotExist(ItemInfo, record, ResultRecord.field, header);
                            OutputBuffer.Enqueue(NewRecord);
                        }
                        foreach (var ReverseNode in ReverseCheckList)
                        {
                            if ((ReverseEdge.Contains(record.RetriveData(ReverseNode)) && record.RetriveData(ReverseNode + 1).Contains(ID)))
                            {
                                Record NewRecord = AddIfNotExist(ItemInfo, record, ResultRecord.field, header);
                                OutputBuffer.Enqueue(NewRecord);
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

        private IQueryable<dynamic> FectNode(string script, GraphViewConnection connection)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DocDB_DatabaseId, connection.DocDB_CollectionId), script, QueryOptions);
            return Result;
        }

        internal List<string> RetriveHeader()
        {
            return header;
        }
        private List<Record> ConvertFromBufferAndEmptyIt(Queue<Record> Buffer)
        {
            List<Record> result = new List<Record>();
            while (Buffer.Count != 0) result.Add(Buffer.Dequeue());
            return result;
        }
        private List<string> DecodeAdjacentList(string AdjString)
        {
            List<string> result = new List<string>();
            string temp = AdjString;
            while (temp.Contains(","))
            {
                result.Add(temp.Substring(0, temp.IndexOf(",")));
                temp = temp.Substring(temp.IndexOf(",") + 1, temp.Length - temp.IndexOf(","));
            }
            result.Add(temp);
            return result;
        }
        private bool HasWhereClause(string SelectClause)
        {
            return !(SelectClause.Length < 6 || SelectClause.Substring(SelectClause.Length - 6, 5) == "Where");
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

    internal class FetchNodeOperator : GraphViewOperator
    {
        internal static Record RecordZero;
        private Queue<Record> OutputBuffer;
        private int OutputBufferSize;

        private int StartOfResultField;

        private List<string> header;
        private GraphViewConnection connection;

        private int node;

        private string docDbScript;

        private List<int> ReverseCheckList;

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
            if (OutputBuffer == null)
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count == 1) this.Close();
            if (OutputBuffer.Count != 0)
            {
                return OutputBuffer.Dequeue();
            }
            string script = docDbScript;
            IQueryable<dynamic> Node = (IQueryable<dynamic>)FectNode(script, connection);
            foreach (var item in Node)
            {
                Tuple<string, string, string> ItemInfo = DecodeJObject((JObject)item);
                string ID = ItemInfo.Item1;
                string edges = ItemInfo.Item2;
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
            if (OutputBuffer.Count == 1) this.Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        private IQueryable<dynamic> FectNode(string script, GraphViewConnection connection)
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

    internal class CartesianProductOperator : GraphViewOperator
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
            
            foreach(var ChildOperator in OperatorOnSubGraphs)
            {
                ResultsFromChildrenOperator.Add(new List<Record>());
                Record result = ChildOperator.Next();
                while(result != null && ChildOperator.Status())
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

        private void CartesianProductOnRecord(List<List<Record>> RecordSet, int IndexOfOperator, Record result)
        {
            if (IndexOfOperator == RecordSet.Count)
            {
                OutputBuffer.Enqueue(result);
                return;
            }
            foreach(var record in RecordSet[IndexOfOperator])
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

