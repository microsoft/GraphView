

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using GraphView.GraphViewDBPortal;
    using Newtonsoft.Json.Linq;

    internal enum OperationType
    {
        Insert,
        Delete,
        Update
    }

    internal class TransactionEntry : Tuple<object, OperationType, long>
    {
        internal TransactionEntry(object record, OperationType otype, long seqNum)
            : base(record, otype, seqNum) { }

        internal object Record
        {
            get
            {
                return this.Item1;
            }
        }

        internal OperationType OperationType
        {
            get
            {
                return this.Item2;
            }
        }

        internal long SequenceNumber
        {
            get
            {
                return this.Item3;
            }
        }
    }

    public class Transaction
    {
        /// <summary>
        /// Data store for logging
        /// </summary>
        private readonly LogStore logStore;
        /// <summary>
        /// Transaction sequence number assigned to this transaction
        /// </summary>
        private readonly long sequenceNumber;

        /// <summary>
        /// A collection of records (indexed by their Ids) and the operations on them
        /// </summary>
        private Dictionary<string, List<TransactionEntry>> recordAccess;

        public Transaction(LogStore logStore, long sequenceNumber)
        {
            this.logStore = logStore;
            this.sequenceNumber = sequenceNumber;
            this.recordAccess = new Dictionary<string, List<TransactionEntry>>();
        }

        public JObject ReadJson(string recordId)
        {
            throw new NotImplementedException();
        }

        public JObject ReadJson(string recordId, JObject valueFromDataStore)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Tuple<string, JObject>> ReadJson(IEnumerable<string> ridList)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Tuple<string, JObject>> ReadJson(IEnumerable<Tuple<string, JObject>> recordList)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<JObject> ReadJson(RecordQuery recordQuery)
        {
            throw new NotImplementedException();
        }

        public string ReadJsonString(string recordId)
        {
            throw new NotImplementedException();
        }

        public byte[] ReadBytes(string recordId)
        {
            throw new NotImplementedException();
        }
        
        public void WriteJson(JObject record)
        {
            throw new NotImplementedException();
        }

        public void WriteJson(IList<JObject> recordList)
        {
            throw new NotImplementedException();
        }

        public void Commit()
        {
            throw new NotImplementedException();
        }

        public void Abort()
        {
            throw new NotImplementedException();
        }
    }
}
