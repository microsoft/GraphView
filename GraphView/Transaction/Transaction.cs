using System.Runtime.CompilerServices;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Data.Entity;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using System.Runtime.Serialization;

    [Serializable]
    public class TransactionException : Exception
    {
        public TransactionException() { }
        public TransactionException(string message) : base("Error when perform " + message + ".\n") { }

        public TransactionException(string message, Exception innerException) :
            base(message, innerException)
        { }

        protected TransactionException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    internal class ReadSetEntry
    {
        internal long VersionKey { get; private set; }
        internal long BeginTimestamp { get; private set; }
        internal long EndTimestamp { get; private set; }
        internal long TxId { get; private set; }
        internal object Record { get; private set; }
        //if you want to upload a new version during Upload Phase, 
        //the LargestVersionKey will be the versionKey of the new version.
        internal long LargestVersionKey { get; private set; }

        public ReadSetEntry(long versionKey, long beginTimestamp, long endTimestamp, long txId, object record, long largestVersionKey)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.TxId = txId;
            this.Record = record;
            this.LargestVersionKey = largestVersionKey;
        }
    }

    public partial class Transaction
    {
        /// <summary>
        /// Data store for loggingl
        /// </summary>
        private readonly LogStore logStore;

        /// <summary>
        /// Version Db for concurrency control
        /// </summary>
        private readonly VersionDb versionDb;


        /// <summary>
        /// Transaction id assigned to this transaction
        /// </summary>
        private readonly long txId;

        /// <summary>
        /// The status of this transaction.
        /// </summary>
        private TxStatus txStatus;

        private long commitTs;

        private long replaceRecordMaxCommitTs;

        private long beginTimestamp;

        /// <summary>
        /// Read set, using for checking visibility of the versions read.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, ReadSetEntry>> readSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, object>> writeSet;

        /// <summary>
        /// A set of version entries that need to be rolled back upon abortion
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, List<long>>> rollbackSet;

        //only for benchmark test
        public TxStatus Status
        {
            get
            {
                return this.txStatus;
            }
        }

        public long CommitTs
        {
            get
            {
                return this.commitTs;

            }
        }

        public long TxId
        {
            get
            {
                return this.txId;

            }
        }

        public Transaction(LogStore logStore, VersionDb versionDb)
        {
            this.logStore = logStore;
            this.versionDb = versionDb;
            this.readSet = new Dictionary<string, Dictionary<object, ReadSetEntry>>();
            this.writeSet = new Dictionary<string, Dictionary<object, object>>();
            this.rollbackSet = new Dictionary<string, Dictionary<object, List<long>>>();

            this.txId = this.versionDb.InsertNewTx();
            this.txStatus = TxStatus.Ongoing;

            this.commitTs = -1;
            this.replaceRecordMaxCommitTs = -1;
            this.beginTimestamp = 0;
        }

    }

    // For record operations
    public partial class Transaction
    {
        internal void GetBeginTimestamp()
        {
            //Tranverse the readSet to get the begin timestamp
            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    long currentBeginTimestamp = readSet[tableId][recordKey].BeginTimestamp;
                    if (this.beginTimestamp < currentBeginTimestamp)
                    {
                        this.beginTimestamp = currentBeginTimestamp;
                    }
                }
            }
        }

        internal bool UploadLocalWriteRecords()
        {
            foreach (string tableId in this.writeSet.Keys)
            {
                foreach (object recordKey in this.writeSet[tableId].Keys)
                {
                    if (this.readSet.ContainsKey(tableId) && this.readSet[tableId].ContainsKey(recordKey))
                    {
                        if (this.writeSet[tableId][recordKey] != null)
                        {
                            //UPDATE Op.
                            //create and upload the new versionEntry
                            VersionEntry newImageEntry = new VersionEntry(
                                recordKey,
                                this.readSet[tableId][recordKey].LargestVersionKey,
                                -1,
                                -1,
                                this.writeSet[tableId][recordKey],
                                this.txId,
                                0);
                            if (!this.versionDb.UploadNewVersionEntry(tableId, recordKey, newImageEntry.VersionKey,
                                newImageEntry))
                            {
                                return false;
                            }

                            //add the info to the rollbackSet
                            this.AddVersionForRollback(tableId, recordKey, newImageEntry.VersionKey);
                        }
                        //Both UPDATE and DELETE Op.
                        //replace the old version's Begin field, End field and TxId field.
                        //three case:
                        //(1) read [Ts, inf, -1], replace by [Ts, inf, myTxId]
                        //(2) read [Ts, inf, txId1] and tx1's status is Aborted, replace by [Ts, inf, myTxId]
                        //(3) read [-1, -1, txId1] and tx1's status is Committed, replace by [tx1CommitTs, inf, myTxId]

                        //case 1:
                        if (this.readSet[tableId][recordKey].EndTimestamp == long.MaxValue &&
                            this.readSet[tableId][recordKey].TxId == -1)
                        {
                            long versionMaxCommitTs = this.versionDb.ReplaceVersionEntryTxId(
                                tableId,
                                recordKey,
                                this.readSet[tableId][recordKey].VersionKey,
                                this.readSet[tableId][recordKey].BeginTimestamp,
                                long.MaxValue,
                                this.txId,
                                -1);
                            if (versionMaxCommitTs == -1)
                            {
                                return false;
                            }

                            if (this.replaceRecordMaxCommitTs < versionMaxCommitTs)
                            {
                                this.replaceRecordMaxCommitTs = versionMaxCommitTs;
                            }

                            //add the info to the rollbackSet
                            this.AddVersionForRollback(tableId, recordKey, this.readSet[tableId][recordKey].VersionKey);
                        }
                        //case 2:
                        else if (this.readSet[tableId][recordKey].EndTimestamp == long.MaxValue &&
                                 this.readSet[tableId][recordKey].TxId != -1)
                        {
                            //need 2 CAS ops
                            //first try to replace [Ts, inf, txId1] with [Ts, inf, myTxId]
                            //if failed (tx1 may have just finished PostProcessing),
                            //then try to replace [Ts, inf, -1] with [Ts, inf, myTxId]
                            long versionMaxCommitTs = this.versionDb.ReplaceVersionEntryTxId(
                                tableId,
                                recordKey,
                                this.readSet[tableId][recordKey].VersionKey,
                                this.readSet[tableId][recordKey].BeginTimestamp,
                                long.MaxValue,
                                this.txId,
                                this.readSet[tableId][recordKey].TxId);
                            if (versionMaxCommitTs == -1)
                            {
                                versionMaxCommitTs = this.versionDb.ReplaceVersionEntryTxId(
                                    tableId,
                                    recordKey,
                                    this.readSet[tableId][recordKey].VersionKey,
                                    this.readSet[tableId][recordKey].BeginTimestamp,
                                    long.MaxValue,
                                    this.txId,
                                    -1);
                                if (versionMaxCommitTs == -1)
                                {
                                    return false;
                                }
                            }

                            if (this.replaceRecordMaxCommitTs < versionMaxCommitTs)
                            {
                                this.replaceRecordMaxCommitTs = versionMaxCommitTs;
                            }

                            //add the info to the rollbackSet
                            this.AddVersionForRollback(tableId, recordKey, this.readSet[tableId][recordKey].VersionKey);
                        }
                        else
                        {
                            //also need 2 CAS ops
                            //first try to replace [-1, -1, txId1] with [tx1CommitTs, inf, myTxId]
                            //if failed (tx1 may have just finished PostProcessing),
                            //then try to replace [tx1CommitTs, inf, -1] with [tx1CommitTs, inf, myTxId]
                            long versionMaxCommitTs = this.versionDb.ReplaceVersionEntryTxId(
                                tableId,
                                recordKey,
                                this.readSet[tableId][recordKey].VersionKey,
                                this.versionDb.GetTxTableEntry(this.readSet[tableId][recordKey].TxId).CommitTime,
                                long.MaxValue,
                                this.txId,
                                this.readSet[tableId][recordKey].TxId);
                            if (versionMaxCommitTs == -1)
                            {
                                versionMaxCommitTs = this.versionDb.ReplaceVersionEntryTxId(
                                    tableId,
                                    recordKey,
                                    this.readSet[tableId][recordKey].VersionKey,
                                    this.versionDb.GetTxTableEntry(this.readSet[tableId][recordKey].TxId).CommitTime,
                                    long.MaxValue,
                                    this.txId,
                                    -1);
                                if (versionMaxCommitTs == -1)
                                {
                                    return false;
                                }
                            }

                            if (this.replaceRecordMaxCommitTs < versionMaxCommitTs)
                            {
                                this.replaceRecordMaxCommitTs = versionMaxCommitTs;
                            }

                            //add the info to the rollbackSet
                            this.AddVersionForRollback(tableId, recordKey, this.readSet[tableId][recordKey].VersionKey);
                        }
                    }
                    else
                    {
                        //INSERT Op.
                        //create and upload the new version entry.
                        VersionEntry newImageEntry = new VersionEntry(
                            recordKey,
                            1,
                            -1,
                            -1,
                            this.writeSet[tableId][recordKey],
                            this.txId,
                            0);
                        if (!this.versionDb.UploadNewVersionEntry(
                            tableId, 
                            recordKey, 
                            newImageEntry.VersionKey,
                            newImageEntry))
                        {
                            return false;
                        }
                        //add the info to the rollbackSet
                        this.AddVersionForRollback(tableId, recordKey, newImageEntry.VersionKey);
                    }
                }
            }

            return true;
        }

        internal void AddVersionForRollback(string tableId, object recordKey, long versionKey)
        {
            if (!this.rollbackSet.ContainsKey(tableId))
            {
                this.rollbackSet[tableId] = new Dictionary<object, List<long>>();
            }

            if (!this.rollbackSet[tableId].ContainsKey(recordKey))
            {
                this.rollbackSet[tableId][recordKey] = new List<long>();
            }
            this.rollbackSet[tableId][recordKey].Add(versionKey);
        }

        internal bool GetCommitTimestamp()
        {
            //CommitTs >= tx.CommitLowerBound
            //CommitTs >= tx.BeginTimestamp
            //CommitTs >= tx.replaceRecordMaxCommitTs + 1
            long proposalTs = this.replaceRecordMaxCommitTs + 1;
            if (proposalTs < this.beginTimestamp)
            {
                proposalTs = this.beginTimestamp;
            }

            this.commitTs = this.versionDb.SetAndGetCommitTime(this.txId, proposalTs);
            return this.commitTs != -1;
        }

        internal bool Validate()
        {
            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    //CAS1
                    //try to push the version’s maxCommitTs as my CommitTs
                    VersionEntry versionEntry = this.versionDb.UpdateVersionMaxCommitTs(
                        tableId,
                        recordKey,
                        readSet[tableId][recordKey].VersionKey,
                        this.commitTs);
                    if (versionEntry.TxId != -1)
                    {
                        //CAS2
                        //try to push the tx who is locking this version's commitLowerBound to myCommitTs + 1
                        long txCommitTs = this.versionDb.UpdateCommitLowerBound(versionEntry.TxId, this.commitTs + 1);
                        if (txCommitTs == -2)
                        {
                            return false;
                        }
                        //range check
                        if (this.commitTs < versionEntry.BeginTimestamp ||
                            this.commitTs > txCommitTs)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        //range check
                        if (this.commitTs < versionEntry.BeginTimestamp ||
                            this.commitTs > versionEntry.EndTimestamp)
                        {
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        internal void Abort()
        {
            this.txStatus = TxStatus.Aborted;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Aborted);

            this.PostProcessingAfterAbort();
        }

        internal void PostProcessingAfterAbort()
        {
            foreach (string tableId in this.rollbackSet.Keys)
            {
                foreach (object recordKey in this.rollbackSet[tableId].Keys)
                {
                    if (this.rollbackSet[tableId][recordKey].Count() == 2)
                    {
                        //UPDATE Op.
                        //The first entry in the list is old, and the second is new
                        //For old version, try to replace [Ts, inf, myTxId] with [Ts, inf, -1]
                        this.versionDb.ReplaceVersionEntryTxId(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].First(),
                            this.readSet[tableId][recordKey].BeginTimestamp,
                            long.MaxValue,
                            -1,
                            this.txId);
                        //For new version, remove it from DB
                        this.versionDb.DeleteVersionEntry(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].Last());
                    }
                    else if (this.readSet.ContainsKey(tableId) &&
                             this.readSet[tableId].ContainsKey(recordKey))
                    {
                        //DELETE Op.
                        this.versionDb.ReplaceVersionEntryTxId(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].First(),
                            this.readSet[tableId][recordKey].BeginTimestamp,
                            long.MaxValue,
                            -1,
                            this.txId);
                    }
                    else
                    {
                        //INSERT Op.
                        this.versionDb.DeleteVersionEntry(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].First());
                    }
                }
            }
        }

        internal void PostProcessingAfterCommit()
        {
            foreach (string tableId in this.rollbackSet.Keys)
            {
                foreach (object recordKey in this.rollbackSet[tableId].Keys)
                {
                    if (this.rollbackSet[tableId][recordKey].Count() == 2)
                    {
                        //UPDATE Op.
                        //The first entry in the list is old, and the second is new
                        //For old version, just replace [Ts, inf, myTxId] with [Ts, myCommitTs, -1]
                        this.versionDb.ReplaceVersionEntryTxId(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].First(),
                            this.readSet[tableId][recordKey].BeginTimestamp,
                            this.commitTs,
                            -1,
                            this.txId);
                        //For new version, try to replace [-1, -1, myTxId] with [myCommitTs, inf, -1]
                        this.versionDb.ReplaceVersionEntryTxId(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].First(),
                            this.commitTs,
                            long.MaxValue,
                            -1,
                            this.txId);
                    }
                    else if (this.readSet.ContainsKey(tableId) &&
                             this.readSet[tableId].ContainsKey(recordKey))
                    {
                        //DELETE Op.
                        this.versionDb.ReplaceVersionEntryTxId(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].First(),
                            this.readSet[tableId][recordKey].BeginTimestamp,
                            this.commitTs,
                            -1,
                            this.txId);
                    }
                    else
                    {
                        //INSERT Op.
                        this.versionDb.ReplaceVersionEntryTxId(
                            tableId,
                            recordKey,
                            this.rollbackSet[tableId][recordKey].First(),
                            this.commitTs,
                            long.MaxValue,
                            -1,
                            this.txId);
                    }
                }
            }
        }

        internal void Commit()
        {
            if (!this.UploadLocalWriteRecords())
            {
                this.Abort();
                throw new TransactionException("Upload");
            }

            if (!this.GetCommitTimestamp())
            {
                this.Abort();
                throw new TransactionException("Get CommitTs");
            }

            if (!this.Validate())
            {
                this.Abort();
                throw new TransactionException("Validation");
            }

            this.WriteChangetoLog();

            this.txStatus = TxStatus.Committed;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Committed);

            this.PostProcessingAfterCommit();
        }

        internal void WriteChangetoLog()
        {
            throw new NotImplementedException();
        }
    }

    public partial class Transaction
    {
        public void Insert(string tableId, object recordKey, object record)
        {
            //check whether the record is already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) && 
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.Abort();
                    throw new TransactionException("Insert");
                }
                else
                {
                    this.writeSet[tableId][recordKey] = record;
                    return;
                }
            }

            //check whether the record is already exist in the local readSet
            if (this.readSet.ContainsKey(tableId) &&
                this.readSet[tableId].ContainsKey(recordKey))
            {
                this.Abort();
                throw new TransactionException("Insert");
            }

            //add the new record to local writeSet
            if (!this.writeSet.ContainsKey(tableId))
            {
                this.writeSet[tableId] = new Dictionary<object, object>();
            }

            this.writeSet[tableId][recordKey] = record;
        }

        public object Read(string tableId, object recordKey)
        {
            //try to find the object in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                return this.writeSet[tableId][recordKey];
            }

            //try to find the obejct in the local readSet
            if (this.readSet.ContainsKey(tableId) && 
                this.readSet[tableId].ContainsKey(recordKey))
            {
                return this.readSet[tableId][recordKey].Record;
            }

            //try to get the object from DB
            IEnumerable<VersionEntry> versionList = this.versionDb.GetVersionList(tableId, recordKey);

            long largestVersionKey = 0;
            VersionEntry versionEntry = this.GetRecentVersionEntryFromList(versionList, out largestVersionKey);

            if (versionEntry == null)
            {
                return null;
            }

            //add the record to local readSet
            if (!this.readSet.ContainsKey(tableId))
            {
                this.readSet[tableId] = new Dictionary<object, ReadSetEntry>();
            }

            this.readSet[tableId][recordKey] = new ReadSetEntry(
                versionEntry.VersionKey,
                versionEntry.BeginTimestamp,
                versionEntry.EndTimestamp,
                versionEntry.TxId,
                versionEntry.Record,
                largestVersionKey);

            return versionEntry.Record;
        }

        public void Update(string tableId, object recordKey, object record)
        {
            //check whether the record is already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.writeSet[tableId][recordKey] = record;
                }
                else
                {
                    this.Abort();
                    throw new TransactionException("Update");
                }
            }
            //check whether the record is already exist in the local readSet
            else if (this.readSet.ContainsKey(tableId) &&
                     this.readSet[tableId].ContainsKey(recordKey))
            {
                //add the update record to the local writeSet
                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, object>();
                }

                this.writeSet[tableId][recordKey] = record;
            }
            else
            {
                this.Abort();
                throw new TransactionException("Update");
            }
        }

        public void Delete(string tableId, object recordKey)
        {
            //check whether the record is already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.writeSet[tableId][recordKey] = null;
                }
                else
                {
                    this.Abort();
                    throw new TransactionException("Delete");
                }
            }
            //check whether the record is already exist in the local readSet
            else if (this.readSet.ContainsKey(tableId) &&
                this.readSet[tableId].ContainsKey(recordKey))
            {
                //add the delete record to the local writeSet
                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, object>();
                }

                this.writeSet[tableId][recordKey] = null;
            }
            else
            {
                this.Abort();
                throw new TransactionException("Delete");
            }
        }

        public object ReadAndInitialize(string tableId, object recordKey)
        {
            //try to find the object in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                return this.writeSet[tableId][recordKey];
            }

            //try to find the obejct in the local readSet
            if (this.readSet.ContainsKey(tableId) && 
                this.readSet[tableId].ContainsKey(recordKey))
            {
                return this.readSet[tableId][recordKey].Record;
            }

            //try to get the object from DB
            IEnumerable<VersionEntry> versionList = this.versionDb.InitializeAndGetVersionList(tableId, recordKey);

            long largestVersionKey = 0;
            VersionEntry versionEntry = this.GetRecentVersionEntryFromList(versionList, out largestVersionKey);

            if (versionEntry == null)
            {
                return null;
            }

            //add the record to local readSet
            if (!this.readSet.ContainsKey(tableId))
            {
                this.readSet[tableId] = new Dictionary<object, ReadSetEntry>();
            }

            this.readSet[tableId][recordKey] = new ReadSetEntry(
                versionEntry.VersionKey,
                versionEntry.BeginTimestamp,
                versionEntry.EndTimestamp,
                versionEntry.TxId,
                versionEntry.Record,
                largestVersionKey);

            return versionEntry.Record;
        }

        internal VersionEntry GetRecentVersionEntryFromList(IEnumerable<VersionEntry> versionList, out long largestVersionKey)
        {
            largestVersionKey = 0;
            foreach (VersionEntry version in versionList)
            {
                if (this.CheckVisibility(version))
                {
                    largestVersionKey = version.VersionKey;
                    return version;
                }

                if (largestVersionKey < version.VersionKey)
                {
                    largestVersionKey = version.VersionKey;
                }
            }

            return null;
        }

        internal bool CheckVisibility(VersionEntry versionEntry)
        {
            if (versionEntry.EndTimestamp == long.MaxValue &&
                versionEntry.TxId == -1)
            {
                return true;
            }

            if (versionEntry.EndTimestamp == long.MaxValue &&
                versionEntry.TxId != -1 &&
                this.versionDb.GetTxTableEntry(versionEntry.TxId).Status == TxStatus.Aborted)
            {
                return true;
            }

            if (versionEntry.EndTimestamp == -1 &&
                versionEntry.TxId != -1 &&
                this.versionDb.GetTxTableEntry(versionEntry.TxId).Status == TxStatus.Committed)
            {
                return true;
            }

            return false;
        }
    }
}

