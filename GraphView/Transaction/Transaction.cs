using System.Data;
using System.Windows.Forms;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Data.Entity;
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
        /// Data store for loggingl
        /// </summary>
        private readonly LogStore logStore;

        /// <summary>
        /// Transaction table for concurrency control
        /// </summary>
        private readonly SingletonTxTable txTable;

        /// <summary>
        /// Lock free hash table for concurrency control
        /// </summary>
        private readonly LockFreeHashTable hashTable;

        /// <summary>
        /// Transaction id assigned to this transaction
        /// </summary>
        private readonly long txId;

        /// <summary>
        /// Begin timestamp assigned to this transaction
        /// </summary>
        private readonly long beginTimestamp;

        /// <summary>
        /// End timestamp assigned to this transaction
        /// </summary>
        private long endTimestamp;

        /// <summary>
        /// A collection of records (indexed by their Ids) and the operations on them
        /// </summary>
        private Dictionary<string, List<TransactionEntry>> recordAccess;

        public Transaction(long txId, long beginTimestamp, LogStore logStore, LockFreeHashTable hashTable, SingletonTxTable txTable)
        {
            this.txId = txId;
            this.beginTimestamp = beginTimestamp;
            this.logStore = logStore;
            this.hashTable = hashTable;
            this.txTable = txTable;
            this.endTimestamp = long.MinValue;
            this.recordAccess = new Dictionary<string, List<TransactionEntry>>();
        }

        public void InsertJson(string recordId, JObject record)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// read the legal version of record.
        /// </summary>
        public JObject ReadJson(string recordId)
        {
            List<VersionEntry> versionList = this.hashTable.GetScanList(recordId);
            foreach (VersionEntry version in versionList)
            {
                if (!(version.IsBeginTxId || version.IsEndTxId))
                {
                    if (this.beginTimestamp >= version.BeginTimestamp && this.beginTimestamp < version.EndTimestamp)
                    {
                        return version.Record;
                    }
                }
            }
            throw new ObjectNotFoundException();
        }

        public void UpdateJson(string recordId, JObject record)
        {
            throw new NotImplementedException();
        }

        public void DeleteJson(string recordId)
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

        public void WrilteJson(IList<JObject> recordList)
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
