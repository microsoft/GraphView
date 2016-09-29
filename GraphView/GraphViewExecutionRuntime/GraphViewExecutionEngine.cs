using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.CodeDom.Compiler;
using Microsoft.CSharp;
using Microsoft.SqlServer.TransactSql.ScriptDom;

// Add DocumentDB references
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;
using System.Collections;

namespace GraphView
{
    /// <summary>
    /// RawRecord is a data sturcture representing data records flowing from one execution operator to another. 
    /// A data record is a multi-field blob. Each field is currently represented as a string.
    /// The interpretation of a record, i.e., the names of the fields/columns of the record, 
    /// is specified in the data operator producing them.  
    /// 
    /// The fields of a record produced by an execution operator are in two parts: 
    /// the first part contains k triples, each representing a node processed so far. 
    /// A triple describes: 1) the node ID, 2) the node's adjacency list, and 3) the node's reverse adjacency list.
    /// The second part is a list of node/edge properties of the processed nodes, projected by the SELECT clause. 
    /// 
    /// | node1 | node1_adjacency_list | node1_rev_adjacency_list |...| nodeK | nodeK_adjacency_list | nodeK_rev_adjacency_list | property1 | property2 |......
    /// </summary>
    internal class RawRecord
    {
        internal RawRecord()
        { 
        }
        internal RawRecord(RawRecord rhs)
        {
            fieldValues = new List<string>(rhs.fieldValues);
        }
        internal RawRecord(int num)
        {
            fieldValues = new List<string>();
            for (int i = 0; i < num; i++)
            {
                fieldValues.Add("");
            }
        }
        internal string RetriveData(List<string> header,string FieldName)
        {
            if (header.IndexOf(FieldName) == -1) return "";
            else if (fieldValues.Count <= header.IndexOf(FieldName)) return "";
            else return fieldValues[header.IndexOf(FieldName)];
        }
        internal string RetriveData(int index)
        {
            return fieldValues[index];
        }
        internal int RetriveIndex(string value)
        {
            if (fieldValues.IndexOf(value) == -1) return -1;
            else return fieldValues.IndexOf(value);
        }
        internal String RetriveRow()
        {
            String row = "";
            if (fieldValues == null) return row;
            for(int i = 0; i < fieldValues.Count; i++)
            {
                row += fieldValues[i].ToString() + ",";
            }
            return row;
        }
        internal List<string> fieldValues;
    }

    /// <summary>
    /// Record differs from RawRecord in that the field names of the blob is annotated. 
    /// It is hence comprehensible to external data readers.  
    /// </summary>
    public class Record
    {
        RawRecord rawRecord;

        internal Record(RawRecord rhs, List<string> pHeader)
        {
            if (rhs != null)
            {
                rawRecord = rhs;
                header = pHeader;
            }
        }
        internal List<string> header { get; set; }
        public string this[int index]
        {
            get
            {
                if (index >= rawRecord.fieldValues.Count)
                    throw new IndexOutOfRangeException("Out of range," + "the Record has only " + rawRecord.fieldValues.Count + " fields");
                else return rawRecord.fieldValues[index];
            }
        }

        public string this[string FieldName]
        {
            get
            {
                if (header == null || header.IndexOf(FieldName) == -1) 
                    throw new IndexOutOfRangeException("Out of range," + "the Record has no field \"" + FieldName + "\".");
                else return rawRecord.fieldValues[header.IndexOf(FieldName)];
            }
        }
    }
    
    /// <summary>
    /// The interface of query execution operators.
    /// An operator is in one of the states: open or closed. 
    /// By implementing Next(), a query execution operator implements its own computation logic 
    /// and returns result iteratively. 
    /// </summary>
    internal interface IGraphViewExecution
    {
        bool State();
        void Open();
        void Close();
        RawRecord Next();
    }
    /// <summary>
    /// The base class for all query execution operators. 
    /// The class implements the execution interface and specifies the field names of 
    /// the raw records produced by this operator. 
    /// </summary>
    internal abstract class GraphViewExecutionOperator : IGraphViewExecution
    {
        private bool state;
        public bool State()
        {
            return state;
        }
        public void Open()
        {
            state = true;
        }
        public void Close()
        {
            state = false;
        }
        public abstract RawRecord Next();

        public List<string> header;
    }
}
