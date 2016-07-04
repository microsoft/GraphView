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
        internal Record()
        { 
        }
        internal Record(int num)
        {
            for (int i = 0; i < num; i++)
            {
                field.Add("");
            }
        }
        internal string RetriveData(List<string> header,string index)
        {
            if (header.IndexOf(index) == -1) return null;
            else if (field.Count <= header.IndexOf(index)) return null;
            else return field[header.IndexOf(index)];
        }

        internal int RetriveIndex(string value)
        {
            if (field.IndexOf(value) == -1) return -1;
            else return field.IndexOf(value);
        }
        public List<string> field;
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
    public abstract class GraphViewOperator : IGraphViewProcessor
    {
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
