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
using System.Collections;

namespace GraphView
{
    /// <summary>
    /// Record is a raw data sturcture flowing from one data operator to another. 
    /// The interpretation of the record is specified in a data operator or a table. 
    /// 
    /// Given a field name, returns the field's value.
    /// Given a field offset, returns the field's value.
    /// </summary>
    public class RawRecord
    {
        internal RawRecord()
        { 
        }
        internal RawRecord(RawRecord rhs)
        {
            field = new List<string>(rhs.field);
        }
        internal RawRecord(int num)
        {
            field = new List<string>();
            for (int i = 0; i < num; i++)
            {
                field.Add("");
            }
        }
        internal string RetriveData(List<string> header,string FieldName)
        {
            if (header.IndexOf(FieldName) == -1) return "";
            else if (field.Count <= header.IndexOf(FieldName)) return "";
            else return field[header.IndexOf(FieldName)];
        }
        internal string RetriveData(int index)
        {
            return field[index];
        }
        internal int RetriveIndex(string value)
        {
            if (field.IndexOf(value) == -1) return -1;
            else return field.IndexOf(value);
        }
        internal String RetriveRow()
        {
            String row = "";
            if (field == null) return row;
            for(int i = 0; i < field.Count; i++)
            {
                row += field[i].ToString() + ",";
            }
            return row;
        }
        internal List<string> field;
    }

    public class Record : RawRecord
    {
        internal Record(RawRecord rhs, List<string> pHeader)
        {
            field = rhs.field;
            header = pHeader;
        }
        internal List<string> header { get; set; }
        public string this[int index]
        {
            get { return field[index]; }
        }

        public string this[string FieldName]
        {
            get
            {
                if (header == null || header.IndexOf(FieldName) == -1) return null;
                return field[header.IndexOf(FieldName)];
            }
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
        RawRecord Next();
    }
    /// <summary>
    /// The most basic class for all operator processor function,
    /// which implements some of the basic interface.
    /// and provides some useful sturcture like buffer on both input and output sides
    /// </summary>
    internal abstract class GraphViewOperator : IGraphViewProcessor
    {
        private bool status;
        public bool Status()
        {
            return status;
        }
        public void Open()
        {
            status = true;
        }
        public void Close()
        {
            status = false;
        }
        public abstract RawRecord Next();

        public List<string> header;
    }
}
