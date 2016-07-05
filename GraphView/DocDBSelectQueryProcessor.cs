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
    internal class TraversalProcessor : GraphViewOperator
    {
        internal static Record RecordZero;
        private Queue<Record> InputBuffer;
        private Queue<Record> OutputBuffer;
        private int InputBufferSize;
        private int OutputBufferSize;
        private GraphViewOperator ChildProcessor;

        //private string InRangeScript = "";
        private int StartOfResultField;

        private List<string> header;
        private GraphViewConnection connection;

        private string src;
        private string dest;

        public TraversalProcessor(GraphViewConnection pConnection,TraversalProcessor pChildProcessor, string pSrc, string pDest, List<string> pheader, int pStartOfResultField, int pInputBufferSize, int pOutputBufferSize)
        {
            this.Open();
            ChildProcessor = pChildProcessor;
            connection = pConnection;
            InputBufferSize = pInputBufferSize;
            OutputBufferSize = pOutputBufferSize;
            InputBuffer = new Queue<Record>();
            InputBuffer = new Queue<Record>();
            src = pSrc;
            dest = pDest;
            header = pheader;
            StartOfResultField = pStartOfResultField;
            if (RecordZero == null) RecordZero = new Record(pheader.Count);
        }
        override public Record Next()
        {
            if (OutputBuffer == null )
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (ChildProcessor != null && !ChildProcessor.Status())))
            {
                return OutputBuffer.Dequeue();
            }

            if (ChildProcessor == null && this.Status())
            {
                if (OutputBuffer.Count == 0) InputBuffer.Enqueue(RecordZero);
            }
            else
                while (InputBuffer.Count() < InputBufferSize && ChildProcessor.Status())
                {
                    if (ChildProcessor != null && ChildProcessor.Status())
                    {
                        Record Result = (Record)ChildProcessor.Next();
                        if (Result == null) ChildProcessor.Close();
                        else
                            InputBuffer.Enqueue(Result);
                    }
                }
            string InRangeScript = "";
            foreach (Record record in InputBuffer)
            {
                if (record.RetriveData(header, src + "_ADJ") !="") InRangeScript += record.RetriveData(header, src + "_ADJ") + ",";
            }
            InRangeScript = CutTheTail(InRangeScript);
            if (InputBuffer.Count != 0)
            {
                string script = InputBuffer.Peek().RetriveData(header, dest + "_SEG");
                if (src != "" && InRangeScript != "") script += " AND " + dest + ".id IN (" + InRangeScript + ")";
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
                        if (src == "" || (ReverseEdge.Contains(record.RetriveData(header, src)) && record.RetriveData(header, src + "_ADJ").Contains(edges)))
                        {
                            Record NewRecord = AddIfNotExist(ItemInfo, record, ResultRecord.field, header);
                            OutputBuffer.Enqueue(NewRecord);
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
        private List<Record> ConvertFromBufferAndEmptyIt(Queue<Record> Buffer)
        {
            List<Record> result = new List<Record>();
            while (Buffer.Count != 0) result.Add(Buffer.Dequeue());
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
            if (NewRecord.RetriveData(header, dest) == "") NewRecord.field[header.IndexOf(dest)] = ItemInfo.Item1;
            if (NewRecord.RetriveData(header, dest+"_ADJ") == "") NewRecord.field[header.IndexOf(dest+"_ADJ")] = ItemInfo.Item2;
            for (int i = 0; i < NewRecord.field.Count; i++) {
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
}

