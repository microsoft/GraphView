
namespace GraphView.Transaction
{
    using System;
    using System.Diagnostics;
    using System.Collections.Generic;
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

        public ReadSetEntry(long versionKey, long beginTimestamp, long endTimestamp, long txId, object record)
        {
            this.VersionKey = versionKey;
            this.BeginTimestamp = beginTimestamp;
            this.EndTimestamp = endTimestamp;
            this.TxId = txId;
            this.Record = record;
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

        /// <summary>
        /// Maximal commit timestamp of all tx's that have updated the write-set records
        /// </summary>
        private long maxCommitTsOfWrites;

        private long beginTimestamp;

        private static readonly long UNSET_TX_COMMIT_TIMESTAMP = -2L;

        private static readonly long DEFAULT_VERSION_TXID_FIELD = -1L;

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
        /// The Tuple stores the beginTs field, endTs field which wanted to be rolled back to.
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, Dictionary<long, Tuple<long, long>>>> abortSet;

        /// <summary>
        /// A set of version entries that need to be changed upon commit
        /// The Tuple stores the beginTs field, endTs field which wanted to be changed to.
        /// The beginTs field and endTs field in the tuple maybe set to -2 temporarily, because we don not get the current tx's commitTs
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, Dictionary<long, Tuple<long, long>>>> commitSet;

        private readonly Dictionary<string, Dictionary<object, long>> largestVersionKeyMap;

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
            this.abortSet = new Dictionary<string, Dictionary<object, Dictionary<long, Tuple<long, long>>>>();
            this.commitSet = new Dictionary<string, Dictionary<object, Dictionary<long, Tuple<long, long>>>>();
            this.largestVersionKeyMap = new Dictionary<string, Dictionary<object, long>>();

            this.txId = this.versionDb.InsertNewTx();
            this.txStatus = TxStatus.Ongoing;

            this.commitTs = -1;
            this.maxCommitTsOfWrites = -1;
            this.beginTimestamp = -1;
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
                    object writeRecord = this.writeSet[tableId][recordKey];
                    if (this.readSet.ContainsKey(tableId) && this.readSet[tableId].ContainsKey(recordKey))
                    {
                        // Upload the new version to the k-v store, when the new version has a payload. 
                        if (this.writeSet[tableId][recordKey] != null)
                        {
                            VersionEntry newImageEntry = new VersionEntry(
                                recordKey,
                                this.largestVersionKeyMap[tableId][recordKey] + 1,
                                VersionEntry.DEFAULT_BEGIN_TIMESTAMP,
                                VersionEntry.DEFAULT_END_TIMESTAMP,
                                writeRecord,
                                this.txId,
                                VersionEntry.DEFAULT_MAX_COMMIT_TS);
                            if (!this.versionDb.UploadNewVersionEntry(tableId, recordKey, newImageEntry.VersionKey,
                                newImageEntry))
                            {
                                return false;
                            }

                            //add the info to the abortSet
                            this.AddVersionToAbortSet(tableId, recordKey, newImageEntry.VersionKey, -1, -1);
                            //add the info to the commitSet
                            this.AddVersionToCommitSet(tableId, recordKey, newImageEntry.VersionKey, 
                                Transaction.UNSET_TX_COMMIT_TIMESTAMP, long.MaxValue);
                        }

                        ReadSetEntry readVersion = this.readSet[tableId][recordKey];

                        // Appends the new version to the tail of the version list
                        // by pinning the tx to the tail version entry. 
                        // The tail entry could be [Ts, inf, -1], [Ts, inf, txId1] or [-1, -1, txId1].
                        // The first case indicates that no concurrent tx is locking the tail.
                        // The second case indicates that one concurrent tx is holding the tail. 
                        // The third case means that a concurrent tx is creating a new tail, which was seen by this tx. 

                        // Tries to hold a lock on the tail when the tail is [Ts, inf, -1]
                        VersionEntry versionEntry = this.versionDb.ReplaceVersionEntryTxId(
                                tableId,
                                recordKey,
                                readVersion.VersionKey,
                                readVersion.BeginTimestamp,
                                long.MaxValue,
                                this.txId,
                                Transaction.DEFAULT_VERSION_TXID_FIELD,
                                long.MaxValue);

                        long rolledBackBegin = versionEntry.BeginTimestamp;
                        this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, versionEntry.MaxCommitTs);

                        // The first try was unsuccessful because the tail is hold by another concurrent tx. 
                        // If the concurrent tx has finished (committed or aborted), there is a chance for this tx
                        // to re-gain the lock. 
                        if (versionEntry.TxId >= 0)
                        {
                            VersionEntry retryEntry = null;
                            TxTableEntry txEntry = this.versionDb.GetTxTableEntry(versionEntry.TxId);

                            // The tx has not finished. Always abort.
                            if (txEntry.Status == TxStatus.Ongoing)
                            {
                                return false;
                            }

                            // The new tail was created by the concurrent tx, yet has not been post-processed. 
                            // The current tx tries to update the tail to the post-processing image and obtain the lock.
                            if (versionEntry.EndTimestamp == -1)
                            {
                                // Only if a new tail's owner tx has been committed, can it be seen by 
                                // the current tx. 
                                Debug.Assert(txEntry.Status == TxStatus.Committed);

                                retryEntry = this.versionDb.ReplaceVersionEntryTxId(
                                    tableId,
                                    recordKey,
                                    versionEntry.VersionKey,
                                    txEntry.CommitTime,
                                    long.MaxValue,
                                    this.txId,
                                    txEntry.TxId,
                                    -1);
                            }
                            // The old tail was locked by a concurrent tx, which has finished but has not cleared the lock. 
                            // The current lock tries to replaces the lock's owner to itself. 
                            else if (versionEntry.EndTimestamp == long.MaxValue)
                            {
                                // The owner tx of the lock has committed. This version entry is not the tail anymore.
                                if (txEntry.Status == TxStatus.Committed)
                                {
                                    return false;
                                }
                                else if (txEntry.Status == TxStatus.Aborted)
                                {
                                    retryEntry = this.versionDb.ReplaceVersionEntryTxId(
                                        tableId,
                                        recordKey,
                                        versionEntry.VersionKey,
                                        versionEntry.BeginTimestamp,
                                        long.MaxValue,
                                        this.txId,
                                        txEntry.TxId,
                                        long.MaxValue);
                                }
                            }
                            
                            if (retryEntry.TxId != this.txId)
                            {
                                return false;
                            }

                            rolledBackBegin = retryEntry.BeginTimestamp;
                            this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, retryEntry.MaxCommitTs);
                        }
                        else
                        {
                            // The new version is failed to append to the tail of the version list, 
                            // because the old tail seen by this tx is not the tail anymore 
                            return false;
                        }

                        // Add the updated tail to the abort set
                        this.AddVersionToAbortSet(tableId, recordKey, readVersion.VersionKey,
                            rolledBackBegin, long.MaxValue);
                        // Add the updated tail to the commit set
                        this.AddVersionToCommitSet(tableId, recordKey, readVersion.VersionKey,
                            rolledBackBegin, Transaction.UNSET_TX_COMMIT_TIMESTAMP);                     
                    }
                    else
                    {
                        //INSERT Op.
                        //create and upload the new version entry.
                        VersionEntry newImageEntry = new VersionEntry(
                            recordKey,
                            this.largestVersionKeyMap[tableId][recordKey] + 1,
                            VersionEntry.DEFAULT_BEGIN_TIMESTAMP,
                            VersionEntry.DEFAULT_END_TIMESTAMP,
                            writeRecord,
                            this.txId,
                            VersionEntry.DEFAULT_MAX_COMMIT_TS);
                        if (!this.versionDb.UploadNewVersionEntry(
                            tableId, 
                            recordKey, 
                            newImageEntry.VersionKey,
                            newImageEntry))
                        {
                            return false;
                        }
                        //add the info to the abortSet
                        this.AddVersionToAbortSet(tableId, recordKey, newImageEntry.VersionKey, -1, -1);
                        //add the info to the commitSet
                        this.AddVersionToCommitSet(tableId, recordKey, newImageEntry.VersionKey, 
                            Transaction.UNSET_TX_COMMIT_TIMESTAMP, long.MaxValue);
                    }
                }
            }

            return true;
        }

        internal void AddVersionToAbortSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            if (!this.abortSet.ContainsKey(tableId))
            {
                this.abortSet[tableId] = new Dictionary<object, Dictionary<long, Tuple<long, long>>>();
            }

            if (!this.abortSet[tableId].ContainsKey(recordKey))
            {
                this.abortSet[tableId][recordKey] = new Dictionary<long, Tuple<long, long>>();
            }
            this.abortSet[tableId][recordKey][versionKey] = new Tuple<long, long>(beginTs, endTs);
        }

        internal void AddVersionToCommitSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            if (!this.commitSet.ContainsKey(tableId))
            {
                this.commitSet[tableId] = new Dictionary<object, Dictionary<long, Tuple<long, long>>>();
            }

            if (!this.commitSet[tableId].ContainsKey(recordKey))
            {
                this.commitSet[tableId][recordKey] = new Dictionary<long, Tuple<long, long>>();
            }
            this.commitSet[tableId][recordKey][versionKey] = new Tuple<long, long>(beginTs, endTs);
        }

        private bool GetCommitTimestamp()
        {
            //CommitTs >= tx.CommitLowerBound
            //CommitTs >= tx.BeginTimestamp
            //CommitTs >= tx.replaceRecordMaxCommitTs + 1

            long proposedCommitTs = this.maxCommitTsOfWrites + 1;

            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
                    {
                        proposedCommitTs = Math.Max(proposedCommitTs, this.readSet[tableId][recordKey].BeginTimestamp + 1);
                    }
                    else
                    {
                        proposedCommitTs = Math.Max(proposedCommitTs, this.readSet[tableId][recordKey].BeginTimestamp);
                    }
                }
            }

            this.commitTs = this.versionDb.SetAndGetCommitTime(this.txId, proposedCommitTs);
            return this.commitTs != -1;
        }

        internal bool Validate()
        {
            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
                    {
                        continue;
                    }

                    //CAS1
                    //try to push the version’s maxCommitTs as my CommitTs
                    VersionEntry versionEntry = this.versionDb.UpdateVersionMaxCommitTs(
                        tableId,
                        recordKey,
                        readSet[tableId][recordKey].VersionKey,
                        this.commitTs,
                        this.txId);
                    if (versionEntry.TxId != -1)
                    {
                        //CAS2
                        //try to push the tx who is locking this version's commitLowerBound to myCommitTs + 1
                        long txCommitTs = this.versionDb.UpdateCommitLowerBound(versionEntry.TxId, this.commitTs + 1);
                        if (txCommitTs == -2)
                        {
                            return false;
                        }
                        else if (txCommitTs == -1)
                        {
                            //the tx who is locking the version has not gotten its commitTs and I push its commitLowerBound successfully.
                            continue;
                        }
                        else
                        {
                            //the tx who is locking the version has already gotten its commitTs
                            //range check
                            if (this.commitTs > txCommitTs)
                            {
                                return false;
                            }
                        }
                    }
                    else
                    {
                        //range check
                        //if there is a new committed version (tx2 committed it) in the list, 
                        //the current version's endTimestamp must be a timestamp (tx2's commitTs),
                        //so we need to compare the current version's endTimestamp with my CommitTs, 
                        //to ensure we can still read the current version and there is no new version has been committed before I commits
                        if (this.commitTs > versionEntry.EndTimestamp)
                        {
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        // IMPORTANT: change to public only for test
        public void Abort()
        {
            this.txStatus = TxStatus.Aborted;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Aborted);

            this.PostProcessingAfterAbort();
        }

        internal void PostProcessingAfterAbort()
        {
            foreach (string tableId in this.abortSet.Keys)
            {
                foreach (object recordKey in this.abortSet[tableId].Keys)
                {
                    foreach (long versionKey in this.abortSet[tableId][recordKey].Keys)
                    {
                        if (this.abortSet[tableId][recordKey][versionKey].Item1 == -1 &&
                            this.abortSet[tableId][recordKey][versionKey].Item2 == -1)
                        {
                            //this is a new version inserted by this aborted tx, delete it
                            this.versionDb.DeleteVersionEntry(tableId, recordKey, versionKey);
                        }
                        else
                        {
                            this.versionDb.ReplaceVersionEntryTxId(
                                tableId,
                                recordKey,
                                versionKey,
                                this.abortSet[tableId][recordKey][versionKey].Item1,
                                this.abortSet[tableId][recordKey][versionKey].Item2,
                                Transaction.DEFAULT_VERSION_TXID_FIELD,
                                this.txId,
                                long.MaxValue);
                        }
                    }
                }
            }
        }

        internal void PostProcessingAfterCommit()
        {
            foreach (string tableId in this.commitSet.Keys)
            {
                foreach (object recordKey in this.commitSet[tableId].Keys)
                {
                    foreach (long versionKey in this.commitSet[tableId][recordKey].Keys)
                    {
                        if (this.commitSet[tableId][recordKey][versionKey].Item1 == Transaction.UNSET_TX_COMMIT_TIMESTAMP)
                        {
                            //this is a new version inserted by this committed tx, try to change it
                            this.versionDb.ReplaceVersionEntryTxId(
                                tableId,
                                recordKey,
                                versionKey,
                                this.commitTs,
                                this.commitSet[tableId][recordKey][versionKey].Item2,
                                Transaction.DEFAULT_VERSION_TXID_FIELD,
                                this.txId,
                                -1);
                        }
                        else
                        {
                            //this is an old version replaced by myself
                            this.versionDb.ReplaceVersionEntryTxId(
                                tableId,
                                recordKey,
                                versionKey,
                                this.commitSet[tableId][recordKey][versionKey].Item1,
                                this.commitTs,
                                Transaction.DEFAULT_VERSION_TXID_FIELD,
                                this.txId,
                                long.MaxValue);
                        }
                    }
                }
            }
        }

        public void Commit()
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
            // IMPORTANT: only for test
            // throw new NotImplementedException();
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
            VersionEntry versionEntry = this.GetVisibleVersionEntry(versionList, out largestVersionKey);

            if (!this.largestVersionKeyMap.ContainsKey(tableId))
            {
                this.largestVersionKeyMap[tableId] = new Dictionary<object, long>();
            }
            this.largestVersionKeyMap[tableId][recordKey] = largestVersionKey;

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
                versionEntry.Record);

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
                //this version is updatable only if its endTs is inf or -1
                if (this.readSet[tableId][recordKey].EndTimestamp != long.MaxValue &&
                    this.readSet[tableId][recordKey].EndTimestamp != -1)
                {
                    this.Abort();
                    throw new TransactionException("Update");
                }

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
                //this version is deletable only if its endTs is inf
                if (this.readSet[tableId][recordKey].EndTimestamp != long.MaxValue)
                {
                    this.Abort();
                    throw new TransactionException("Delete");
                }

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
            VersionEntry versionEntry = this.GetVisibleVersionEntry(versionList, out largestVersionKey);

            if (!this.largestVersionKeyMap.ContainsKey(tableId))
            {
                this.largestVersionKeyMap[tableId] = new Dictionary<object, long>();
            }
            this.largestVersionKeyMap[tableId][recordKey] = largestVersionKey;

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
                versionEntry.Record);

            return versionEntry.Record;
        }

        /// <summary>
        /// Given a version list, returns a visible version to the tx.
        /// </summary>
        /// <param name="versionList">The version list</param>
        /// <param name="largestVersionKey">The version key of the most-recently committed version entry</param>
        /// <returns>The version entry visible to the tx</returns>
        internal VersionEntry GetVisibleVersionEntry(IEnumerable<VersionEntry> versionList, out long largestVersionKey)
        {
            largestVersionKey = 0;
            VersionEntry visibleVersion = null;

            foreach (VersionEntry versionEntry in versionList)
            {
                TxTableEntry pendingTxEntry = null;
                TxStatus pendingTxStatus = TxStatus.Committed;
                // If the version entry is a dirty write, skips the entry
                if (versionEntry.TxId >= 0)
                {
                    pendingTxEntry = this.versionDb.GetTxTableEntry(versionEntry.TxId);
                    pendingTxStatus = pendingTxEntry.Status;

                    if (versionEntry.EndTimestamp == -1)
                    {
                        if (pendingTxStatus == TxStatus.Ongoing || pendingTxStatus == TxStatus.Aborted)
                        {
                            continue;
                        }
                    }
                }

                if (visibleVersion == null)
                {
                    if (versionEntry.TxId == -1)
                    {
                        if (this.beginTimestamp >= 0)
                        {
                            // When a tx has a begin timestamp after intialization
                            if (this.beginTimestamp >= versionEntry.BeginTimestamp && this.beginTimestamp < versionEntry.EndTimestamp)
                            {
                                visibleVersion = versionEntry;
                            }
                        }
                        else
                        {
                            // When a tx has no begin timestamp after intialization, the tx is under serializability. 
                            // A read always returns the most-recently committed version.
                            if (versionEntry.EndTimestamp == long.MaxValue)
                            {
                                visibleVersion = versionEntry;
                            }
                        }
                    }
                    else if (versionEntry.TxId >= 0)
                    {
                        // A dirty write has been appended after this version entry. 
                        // This version is visible if the writing tx has not been committed 
                        if (versionEntry.EndTimestamp == long.MaxValue && pendingTxStatus != TxStatus.Committed)
                        {
                            visibleVersion = versionEntry;
                        }
                        // A dirty write is visible to this tx when the writing tx has been committed, 
                        // which has not finished postprocessing and changing the dirty write to a normal version entry
                        else if (versionEntry.EndTimestamp == -1 && pendingTxStatus == TxStatus.Committed)
                        {
                            visibleVersion = new VersionEntry(
                                versionEntry.RecordKey, 
                                versionEntry.VersionKey, 
                                pendingTxEntry.CommitTime, 
                                long.MaxValue, 
                                versionEntry.Record, 
                                -1, 
                                versionEntry.MaxCommitTs);
                        }
                    }
                }

                if (largestVersionKey < versionEntry.VersionKey)
                {
                    largestVersionKey = versionEntry.VersionKey;
                }
            }

            return visibleVersion;
        }
    }
}

