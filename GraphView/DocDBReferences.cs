using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class Record
    {
        internal Record()
        {
            Bindings = new List<string>();
            Results = new List<string>();
        }
        internal Record(List<string> pBindings, List<string> pResults)
        {
            Bindings = pBindings;
            Results = pResults;
        }
        public int GetBinding(string pId, List<int> pBindingHeader)
        {
            if (Bindings.IndexOf(pId) == -1) return -1;
            return pBindingHeader[Bindings.IndexOf(pId)];
        }
        public string GetId(string ResultIndex, List<string> pResultHeader)
        {
            if (pResultHeader.IndexOf(ResultIndex) == -1) return "";
            return Results[pResultHeader.IndexOf(ResultIndex)];
        }
        public string GetId(int Binding, List<int> pBindingHeader)
        {
            if (pBindingHeader.IndexOf(Binding) == -1) return "";
            return Bindings[pBindingHeader.IndexOf(Binding)];
        }

        public List<string> Bindings;
        public List<string> Results;
    }

    internal class Table
    {
        internal List<int> BindingIndex;
        internal List<string> ResultsIndex;
        internal List<Record> records;
        internal int RecordIndex;
        public int Depth
        {
            get
            {
                throw new NotImplementedException();
            }
        }
        public int FieldCount
        {
            get
            {
                return BindingIndex.Count + ResultsIndex.Count;
            }
        }
        public bool IsClosed
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int RecordsAffected
        {
            get
            {
                throw new NotImplementedException();
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
                return records[RecordIndex].Results[ResultsIndex.IndexOf(index)];
            }
        }
        internal Table(List<int> pBindingIndex, List<string> pResultsIndex)
        {
            BindingIndex = pBindingIndex;
            ResultsIndex = pResultsIndex;
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
            return records[RecordIndex].Results[ResultsIndex.IndexOf(ResultIndex)];
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
    internal interface DocDBOperator
    {
        void Open();
        void Close();
        object Next();
    }

    internal abstract class DocDBOperatorProcessor : DocDBOperator
    {
        internal Table InputBuffer;
        internal Table OutputBuffer;
        internal int InputBufferSize;
        internal int OutputBufferSize;
        internal List<DocDBOperatorProcessor> ChildrenProcessor;
        internal bool statue;
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
