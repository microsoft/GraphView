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
        /// The status of this transaction.
        /// </summary>
        private TxStatus txStatus;

        /// <summary>
        /// Read set, using for checking visibility of the versions read.
        /// For every read operation, add the recordId, the begin and the end timestamp of the version we read to the readSet.
        /// </summary>
        private readonly List<ReadSetEntry> readSet;

        /// <summary>
        /// Scan set, using for checking phantoms.
        /// To do a index scan, a transaction T specifies an index I, a predicate P, abd a logical read time RT.
        /// We only have one index (recordId) currently, just add the recordId and the readTimestamp to the scanSet.
        /// </summary>
        private readonly List<ScanSetEntry> scanSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// </summary>
        private readonly List<WriteSetEntry> writeSet;

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
            this.txStatus = TxStatus.Active;
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
                Console.WriteLine("Insert failed. There is already a version with the same versionKey.");
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
                Console.WriteLine("Read failed.");
                this.Abort();
                return null;
            }
            this.readSet.Add(new ReadSetEntry(versionKey, version.BeginTimestamp, version.EndTimestamp));
            return version.Record;
        }

        /// <summary>
        /// Update a record.
        /// </summary>
        public void UpdateJson(VersionKey versionKey, JObject record, long readTimestamp)
        {
            this.scanSet.Add(new ScanSetEntry(versionKey, readTimestamp));
            VersionEntry oldVersion = null;
            VersionEntry newVersion = null;
            if (!this.versionTable.UpdateVersion(versionKey, record, this.txId, readTimestamp, out oldVersion, out newVersion))
            {
                //update failed, two situation:
                if (oldVersion != null)
                {
                    Console.WriteLine("Update failed. Other transaction has already set the version's end field.");
                }
                else
                {
                    Console.WriteLine("Update failed. Can not find the legal version to perform update.");
                }
                Abort();
                return;
            }
            //update successfully, two situation:
            if (oldVersion != null)
            {
                this.writeSet.Add(new WriteSetEntry(versionKey, oldVersion.BeginTimestamp, oldVersion.EndTimestamp, true));
                this.writeSet.Add(new WriteSetEntry(versionKey, newVersion.BeginTimestamp, newVersion.EndTimestamp, false));
            }
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
                Console.WriteLine("Delete failed. Other transaction has already set the version's end field.");
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

        /// <summary>
        /// The transaction scans its ReadSet and for each version read, 
        /// checks whether the version is still visible at the end of the transaction.
        /// </summary>
        internal bool ReadValidation()
        {
            foreach (ReadSetEntry readSetEntry in this.readSet)
            {
                if (!this.versionTable.CheckVersionVisibility(readSetEntry.Key, readSetEntry.BeginTimestamp, this.endTimestamp))
                {
                    Console.WriteLine("Read validation failed.");
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// The transaction walks its ScanSet and repeats each scan,
        /// looking for versions that came into existence during T’s lifetime and are visible as of the end of the transaction.
        /// </summary>
        internal bool PhantomValidation()
        {
            foreach (ScanSetEntry scanSetEntry in this.scanSet)
            {
                if (!this.versionTable.CheckPhantom(scanSetEntry.Key, scanSetEntry.ReadTimestamp, this.endTimestamp))
                {
                    Console.WriteLine("Check phantom failed.");
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Write changes to LogStore.
        /// </summary>
        internal void WriteChangestoLog()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// After complete all its normal processing, the transaction first acquires a end timestamp, then,
        /// checks visibility of the versions read, checks for phantoms,
        /// writes the new versions it created, and info about the deleted version to a persistent log,
        /// propagates its end timestamp to the Begin and End fields of new and old versions, respectively, listed in its writeSet.
        /// </summary>
        public void Commit(long endTimestamp)
        {
            this.endTimestamp = endTimestamp;
            //validation
            if (!this.ReadValidation() || !this.PhantomValidation())
            {
                this.Abort();
                return;
            }
            //logging
            this.WriteChangestoLog();
            //propagates endtimestamp to versionTable
            foreach (WriteSetEntry writeSetEntry in this.writeSet)
            {
                if (!this.versionTable.UpdateCommittedVersionTimestamp(writeSetEntry.Key, this.txId, this.endTimestamp, writeSetEntry.IsOld))
                {
                    this.Abort();
                    return;
                }
            }
            //change the transaction's status
            this.txStatus = TxStatus.Committed;
            this.txTable.UpdateTxEndTimestampByTxId(this.txId, this.endTimestamp);
            this.txTable.UpdateTxStatusByTxId(this.txId, TxStatus.Committed);
        }

        /// <summary>
        /// Abort this transaction.
        /// </summary>
        public void Abort()
        {
            //update all changed version's timestamp
            foreach (WriteSetEntry writeSetEntry in this.writeSet)
            {
                this.versionTable.UpdateAbortedVersionTimestamp(writeSetEntry.Key, this.txId, writeSetEntry.IsOld);
            }
            //change the transaction's status
            this.txStatus = TxStatus.Aborted;
            this.txTable.UpdateTxStatusByTxId(this.txId, TxStatus.Aborted);
        }
    }
}
