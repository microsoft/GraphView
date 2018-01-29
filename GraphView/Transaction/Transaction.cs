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

    internal class ReadSetEntry : Tuple<VersionKey, long, long>
    {
        internal ReadSetEntry(VersionKey key, long beginTimestamp, long endTimestamp)
            : base(key, beginTimestamp, endTimestamp) { }

        internal VersionKey Key
        {
            get
            {
                return this.Item1;
            }
        }

        internal long BeginTimestamp
        {
            get
            {
                return this.Item2;
            }
        }

        internal long EndTimestamp
        {
            get
            {
                return this.Item3;
            }
        }
    }

    internal class ScanSetEntry : Tuple<VersionKey, long>
    {
        internal ScanSetEntry(VersionKey key, long readTimestamp)
            : base(key, readTimestamp) { }

        internal VersionKey Key
        {
            get
            {
                return this.Item1;
            }
        }

        internal long ReadTimestamp
        {
            get { return this.Item2; }
        }
    }

    internal class WriteSetEntry : Tuple<VersionKey, long, long, bool>
    {
        internal WriteSetEntry(VersionKey key, long beginTimestamp, long endTimestamp, bool isOld)
            : base(key, beginTimestamp, endTimestamp, isOld) { }

        internal VersionKey Key
        {
            get
            {
                return this.Item1;
            }
        }

        internal long BeginTimestamp
        {
            get
            {
                return this.Item2;
            }
        }

        internal long EndTimestamp
        {
            get
            {
                return this.Item3;
            }
        }

        internal bool IsOld
        {
            get
            {
                return this.Item4;
            }
        }
    }

    public class VersionKey : Tuple<string, string>
    {
        internal VersionKey(string tableId, string recordId)
            : base(tableId, recordId) { }

        internal string TableId
        {
            get
            {
                return this.Item1;
            }
        }

        internal string RecordId
        {
            get
            {
                return this.Item2;
            }
        }

        public override bool Equals(object obj)
        {
            if (obj == null || obj.GetType() != this.GetType())
                return false;
            VersionKey other = (VersionKey) obj;
            return (this.Item1 == other.Item1) && (this.Item2 == other.Item2);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }

    public class Transaction
    {
        /// <summary>
        /// Data store for loggingl
        /// </summary>
        private readonly LogStore logStore;

        /// <summary>
        /// Version table for concurrency control
        /// </summary>
        private readonly IVersionTable versionTable;

        /// <summary>
        /// Transaction table, keeping track of each transcation's status 
        /// </summary>
        private readonly ITxTable txTable;

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
        /// Read set, using for checking visibility of the versions read.
        /// For every read operation, add the recordId, the begin and the end timestamp of the version we read to the readSet.
        /// </summary>
        private List<ReadSetEntry> readSet;

        /// <summary>
        /// Scan set, using for checking phantoms.
        /// To do a index scan, a transaction T specifies an index I, a predicate P, abd a logical read time RT.
        /// We only have one index (recordId) currently, just add the recordId and the readTimestamp to the scanSet.
        /// </summary>
        private List<ScanSetEntry> scanSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// </summary>
        private List<WriteSetEntry> writeSet;

        /// <summary>
        /// A collection of records (indexed by their Ids) and the operations on them
        /// </summary>
        private Dictionary<string, List<TransactionEntry>> recordAccess;

        public Transaction(long txId, long beginTimestamp, LogStore logStore, IVersionTable versionTable, ITxTable txTable)
        {
            this.txId = txId;
            this.beginTimestamp = beginTimestamp;
            this.logStore = logStore;
            this.versionTable = versionTable;
            this.txTable = txTable;

            this.endTimestamp = long.MinValue;
            this.recordAccess = new Dictionary<string, List<TransactionEntry>>();

            this.readSet = new List<ReadSetEntry>();
            this.scanSet = new List<ScanSetEntry>();
            this.writeSet = new List<WriteSetEntry>();

            this.txTable.InsertNewTx(this.txId, this.beginTimestamp);
        }
        
        /// <summary>
        /// Insert a new record.
        /// (1) Add the scan info to the scan set
        /// (2) Find whether the record already exist.
        /// (3) Insert and add info to write set, or, abort.
        /// </summary>
        public void InsertJson(VersionKey versionKey, JObject record, long readTimestamp)
        {
            this.scanSet.Add(new ScanSetEntry(versionKey, readTimestamp));
            if (!this.versionTable.InsertVersion(versionKey, record, this.txId, readTimestamp))
            {
                //insert failed, because there is already a version with the same versionKey
                this.Abort();
                return;
            }
            //insert successfully
            this.writeSet.Add(new WriteSetEntry(versionKey, this.txId, long.MaxValue, false));
        }

        /// <summary>
        /// Read the legal record.
        /// (1) Add the scan info to the scanSet.
        /// (2) Try to get the legal version from versionTable.
        /// (3) Add the read info to the readSet, or, abort.
        /// </summary>
        public JObject ReadJson(VersionKey versionKey, long readTimestamp)
        {
            this.scanSet.Add(new ScanSetEntry(versionKey, readTimestamp));
            VersionEntry version = this.versionTable.GetVersion(versionKey, readTimestamp);
            if (version == null)
            {
                //can not find the record
                this.Abort();
                return null;
            }
            this.readSet.Add(new ReadSetEntry(versionKey, version.BeginTimestamp, version.EndTimestamp));
            return version.Record;
        }

        public void UpdateJson(VersionKey versionKey, JObject record)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Delete a record.
        /// </summary>
        public void DeleteJson(VersionKey versionKey, long readTimestamp)
        {
            this.scanSet.Add(new ScanSetEntry(versionKey, readTimestamp));
            VersionEntry deletedVersion = null;
            if (!this.versionTable.DeleteVersion(versionKey, this.txId, readTimestamp, out deletedVersion))
            {
                this.Abort();
                return;
            }
            //delete successfully
            if (deletedVersion != null)
            {
                this.writeSet.Add(new WriteSetEntry(versionKey, deletedVersion.BeginTimestamp, deletedVersion.EndTimestamp, true));
            }
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

        /// <summary>
        /// Abort this transaction.
        /// This method is NOT fully completed.
        /// </summary>
        public void Abort()
        {
            this.txTable.UpdateTxStatusByTxId(this.txId, TxStatus.Aborted);
        }
    }
}
