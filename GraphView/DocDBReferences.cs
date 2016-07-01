using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using Microsoft.CSharp;
using Microsoft.SqlServer.TransactSql.ScriptDom;
// Add DocumentDB references
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;

namespace GraphView
{
    /// <summary>
    /// DocDBConnection is used to manage the connection with server. 
    /// </summary>
    public class DocDBConnection
    {
        public DocDBConnection(int pMaxPacketSize, GraphViewConnection connection)
        {
            MaxPacketSize = pMaxPacketSize;
            EndPointUrl = connection.DocDB_Url;
            PrimaryKey = connection.DocDB_Key;
            DatabaseID = connection.DocDB_DatabaseId;
            CollectionID = connection.DocDB_CollectionId;
            client = connection.client;
        }
        public int MaxPacketSize;
        public string EndPointUrl;
        public string PrimaryKey;
        public string DatabaseID;
        public string CollectionID;
        public DocumentClient client;
    }
    
    /// <summary>
    /// Record is a raw data sturcture flowing from one data operator to another. 
    /// The interpretation of the record is specified in a data operator or a table. 
    /// </summary>
    internal class Record
    {
        private static int ResultNumber = 0;
        internal void ReSetResultNumber(int pResultNumber)
        {
            ResultNumber = pResultNumber;
        }
        internal Record()
        {
            Bindings = new List<string>();
            Fields = new List<string>();
            for (int i = 0; i < ResultNumber; i++) Fields.Add("");
        }
        internal Record(int pResultNumber)
        {
            Bindings = new List<string>();
            Fields = new List<string>();
            for (int i = 0; i < pResultNumber; i++) Fields.Add("");
        }
        internal Record(List<string> pBindings, List<string> pResults)
        {
            Bindings = pBindings;
            Fields = pResults;
        }
        public int GetBinding(string pId, List<int> pBindingHeader)
        {
            if (Bindings.IndexOf(pId) == -1) return -1;
            return pBindingHeader[Bindings.IndexOf(pId)];
        }
        public string GetId(string ResultIndex, List<string> pResultHeader)
        {
            if (pResultHeader.IndexOf(ResultIndex) == -1) return "";
            return Fields[pResultHeader.IndexOf(ResultIndex)];
        }
        public string GetId(int Binding, List<int> pBindingHeader)
        {
            if (Bindings.Count <= pBindingHeader.IndexOf(Binding)) return "";
            return Bindings[pBindingHeader.IndexOf(Binding)];
        }
        public int GetIndex(string Result, List<string> pResultHeader)
        {
            return pResultHeader.IndexOf(Result);
        }
        public List<string> Bindings;
        public List<string> Fields;
    }
    
    /// <summary>
    /// TableBuffer is a buffer that caches a certain number of records. 
    /// </summary>
    internal class TableBuffer
    {
        internal List<int> BindingIndex;
        internal List<string> ResultsIndex;
        internal List<Record> records;
        internal int RecordIndex;
        public int FieldCount
        {
            get
            {
                return BindingIndex.Count + ResultsIndex.Count;
            }
        }
        public object this[Int32 index]
        {
            get
            {
                if (BindingIndex.IndexOf(index) == -1) return null;
                return records[RecordIndex].Bindings[BindingIndex.IndexOf(index)];
            }
        }
        public object this[string index]
        {
            get
            {
                if (ResultsIndex.IndexOf(index) == -1) return "";
                return records[RecordIndex].Fields[ResultsIndex.IndexOf(index)];
            }
        }
        internal TableBuffer(List<int> pBindingIndex, List<string> pResultsIndex)
        {
            BindingIndex = pBindingIndex;
            ResultsIndex = pResultsIndex;
            records = new List<Record>();
        }
        internal void AddRecord(Record r)
        {
            records.Add(r);
        }
        public bool Read()
        {
            if (records.Count == RecordIndex) return false;
            else
            {
                RecordIndex++;
                return true;
            }
        }
        public int GetBinding(string pId)
        {
            if (records[RecordIndex].Bindings.IndexOf(pId) == -1) return -1;
            return BindingIndex[records[RecordIndex].Bindings.IndexOf(pId)];
        }
        public string GetId(string ResultIndex)
        {
            if (ResultsIndex.IndexOf(ResultIndex) == -1) return "";
            return records[RecordIndex].Fields[ResultsIndex.IndexOf(ResultIndex)];
        }
        public string GetId(int Binding)
        {
            if (BindingIndex.IndexOf(Binding) == -1) return "";
            return records[RecordIndex].Bindings[BindingIndex.IndexOf(Binding)];
        }
        public void Dispose()
        {
            BindingIndex.Clear();
            ResultsIndex.Clear();
            records.Clear();
        }
    }

    /// <summary>
    /// DocDBOperator is the basic interface of all operator processor function.
    /// It provides three basic interface about the statue of a operator processor function.
    /// And one interface to execute the operator. 
    /// </summary>
    internal interface IGraphViewProcessor
    {
        bool Status();
        void Open();
        void Close();
        object Next();
    }
    /// <summary>
    /// The most basic class for all operator processor function,
    /// which implements some of the basic interface.
    /// and provides some useful sturcture like buffer on both input and output sides
    /// </summary>
    internal abstract class GraphViewOperator : IGraphViewProcessor
    {
        internal Queue<Record> InputBuffer;
        internal Queue<Record> OutputBuffer;
        internal int InputBufferSize;
        internal int OutputBufferSize;
        internal List<GraphViewOperator> ChildrenProcessor;
        internal bool statue;
        public bool Status()
        {
            return statue;
        }
        public void Open()
        {
            statue = true;
        }
        public void Close()
        {
            statue = false;
        }
        public abstract object Next();
    }
}
