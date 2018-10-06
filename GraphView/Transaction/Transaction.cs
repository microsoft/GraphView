
namespace GraphView.Transaction
{
    using System;
    using System.Diagnostics;
    using System.Collections.Generic;

    public class TransactionException : Exception
    {
        public TransactionException() { }
        public TransactionException(string message) : base($"Error when perform '{message}' message.\n") { }

        public TransactionException(string message, Exception innerException) :
            base(message, innerException)
        { }
    }
    public class AbortException : Exception
    {
        public AbortException() : base("Transaction aborted") { }
        public AbortException(string message) : base($"Transaction Aborted {message}") { }
    }

    public partial class Transaction
    {
        /// <summary>
        /// The default transaction begin timestamp
        /// </summary>
        private static readonly long DEFAULT_TX_BEGIN_TIMESTAMP = -1L;

        /// <summary>
        /// The maximal capcity of the postprocessing entry list
        /// For every record, it will have at most 2 version entries to rollback or commit
        /// </summary>
        private static readonly int POSTPROCESSING_LIST_MAX_CAPACITY = 2;

        /// <summary>
        /// The flag which indicates that the field should be replaced by its commit time during the postprocessing phase
        /// </summary>
        private static readonly long UNSET_TX_COMMIT_TIMESTAMP = -2L;

        /// <summary>
        /// Data store for logging
        /// </summary>
        private readonly ILogStore logStore;

        /// <summary>
        /// Version Db for concurrency control
        /// </summary>
        private readonly VersionDb versionDb;

        /// <summary>
        /// Transaction id assigned to this transaction
        /// </summary>
        private long txId;

        /// <summary>
        /// The status of this transaction.
        /// </summary>
        /// IMPORTANT: only for unit test
        private TxStatus txStatus;

        /// <summary>
        /// Transaction's commit time
        /// </summary>
        private long commitTs;

        /// <summary>
        /// Maximal commit timestamp of all tx's that have updated the write-set records
        /// </summary>
        private long maxCommitTsOfWrites;

        /// <summary>
        /// Transaction's begin time stamp
        /// </summary>
        private long beginTimestamp;

        /// <summary>
        /// Read set, using for checking visibility of the versions read.
        /// Format: tableId => [recordKey => ReadSetEntry]
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, ReadSetEntry>> readSet;

        /// <summary>
        /// Write set, using for
        /// 1) logging new versions during commit
        /// 2) updating the old and new versions' timestamps during commit
        /// 3) locating old versions for garbage collection
        /// Add the versions updated (old and new), versions deleted (old), and versions inserted (new) to the writeSet.
        /// Format: tableId => [recordKey => image]
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, object>> writeSet;

        /// <summary>
        /// A set of version entries that need to be rolled back upon abortion
        /// The Tuple stores the beginTs field, endTs field which wanted to be rolled back to.
        /// Format: tableId => [recordKey => List of Entry]
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, List<PostProcessingEntry>>> abortSet;

        /// <summary>
        /// A set of version entries that need to be changed upon commit
        /// The Tuple stores the beginTs field, endTs field which wanted to be changed to.
        /// The beginTs field and endTs field in the tuple maybe set to -2 temporarily, since we haven't get the current tx's commitTs
        /// tableId => [recordKey => List of Entry]
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, List<PostProcessingEntry>>> commitSet;

        /// <summary>
        /// A set of largest key for every record key to infer the version key for new entry
        /// For the insertion operation, there is no entry in the read set, we
        /// should keep the largest version key in a global set
        /// </summary>
        private readonly Dictionary<string, Dictionary<object, long>> largestVersionKeyMap;

        private readonly List<Tuple<string, object, VersionEntry>> validationVersions;

        //private readonly Queue<Tuple<long, long>> garbageQueue;
        private readonly Queue<long> garbageQueueTxId;
        private readonly Queue<long> garbageQueueFinishTime;

        // only for benchmark test
        public TxStatus Status
        {
            get
            {
                return this.txStatus;
            }
        }

        // IMPORTANT: change to public only for test
        // IMPORTANT: change to get and set only for test
        // only for benchmark test
        public long CommitTs
        {
            get
            {
                return this.commitTs;

            }
            set
            {
                this.commitTs = value;

            }
        }

        // only for benchmark test
        public long TxId
        {
            get
            {
                return this.txId;

            }
        }

        public Transaction(
            ILogStore logStore, 
            VersionDb versionDb, 
            long txId = -1,
            Queue<long> garbageQueueTxId = null,
            Queue<long> garbageQueueFinishTime = null)
        {
            this.logStore = logStore;
            this.versionDb = versionDb;
            this.readSet = new Dictionary<string, Dictionary<object, ReadSetEntry>>();
            this.writeSet = new Dictionary<string, Dictionary<object, object>>();
            this.abortSet = new Dictionary<string, Dictionary<object, List<PostProcessingEntry>>>();
            this.commitSet = new Dictionary<string, Dictionary<object, List<PostProcessingEntry>>>();
            this.largestVersionKeyMap = new Dictionary<string, Dictionary<object, long>>();
            this.validationVersions = new List<Tuple<string, object, VersionEntry>>();

            this.txId = txId < 0 ? this.versionDb.InsertNewTx() : txId;
            this.txStatus = TxStatus.Ongoing;

            this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.maxCommitTsOfWrites = -1L;
            this.beginTimestamp = Transaction.DEFAULT_TX_BEGIN_TIMESTAMP;

            this.garbageQueueTxId = garbageQueueTxId;
            this.garbageQueueFinishTime = garbageQueueFinishTime;
        }

		public void Clear(long txId)
		{
			this.readSet.Clear();
			this.writeSet.Clear();
			this.abortSet.Clear();
			this.commitSet.Clear();
			this.largestVersionKeyMap.Clear();
            this.validationVersions.Clear();

			this.txId = txId;
			this.txStatus = TxStatus.Ongoing;

			this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
			this.maxCommitTsOfWrites = -1L;
			this.beginTimestamp = Transaction.DEFAULT_TX_BEGIN_TIMESTAMP;
		}
    }

    // For record operations
    public partial class Transaction
    { 
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
                                this.largestVersionKeyMap[tableId][recordKey] + 1,
                                writeRecord,
                                this.txId);

                            if (!this.versionDb.UploadNewVersionEntry(tableId, recordKey, newImageEntry.VersionKey,
                                newImageEntry))
                            {
                                return false;
                            }

                            //add the info to the abortSet
                            this.AddVersionToAbortSet(tableId, recordKey, newImageEntry.VersionKey, 
                                VersionEntry.DEFAULT_BEGIN_TIMESTAMP, VersionEntry.DEFAULT_END_TIMESTAMP);
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
                        VersionEntry versionEntry = this.versionDb.ReplaceVersionEntry(
                                tableId,
                                recordKey,
                                readVersion.VersionKey,
                                readVersion.BeginTimestamp,
                                long.MaxValue,
                                this.txId,
                                VersionEntry.EMPTY_TXID,
                                long.MaxValue);

                        long rolledBackBegin = versionEntry.BeginTimestamp;
                        this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, versionEntry.MaxCommitTs);

                        if (versionEntry.TxId == this.TxId)
                        {
                            // the first try was successful, do nothing
                        }
                        // The first try was unsuccessful because the tail is hold by another concurrent tx. 
                        // If the concurrent tx has finished (committed or aborted), there is a chance for this tx
                        // to re-gain the lock. 
                        else if (versionEntry.TxId >= 0)
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
                            if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP)
                            {
                                // Only if a new tail's owner tx has been committed, can it be seen by 
                                // the current tx. Since we are trying to replace the read version entry and lock it,
                                // Thus it must be commited if it's a new version
                                Debug.Assert(txEntry.Status == TxStatus.Committed);

                                retryEntry = this.versionDb.ReplaceVersionEntry(
                                    tableId,
                                    recordKey,
                                    versionEntry.VersionKey,
                                    txEntry.CommitTime,
                                    long.MaxValue,
                                    this.txId,
                                    txEntry.TxId,
                                    VersionEntry.DEFAULT_END_TIMESTAMP);
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
                                    retryEntry = this.versionDb.ReplaceVersionEntry(
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
                            this.largestVersionKeyMap[tableId][recordKey] + 1,
                            writeRecord,
                            this.txId);

                        if (!this.versionDb.UploadNewVersionEntry(
                            tableId, 
                            recordKey, 
                            newImageEntry.VersionKey,
                            newImageEntry))
                        {
                            return false;
                        } 

                        //add the info to the abortSet
                        this.AddVersionToAbortSet(tableId, recordKey, newImageEntry.VersionKey,
                            VersionEntry.DEFAULT_BEGIN_TIMESTAMP, VersionEntry.DEFAULT_END_TIMESTAMP);
                        //add the info to the commitSet
                        this.AddVersionToCommitSet(tableId, recordKey, newImageEntry.VersionKey, 
                            Transaction.UNSET_TX_COMMIT_TIMESTAMP, long.MaxValue);
                    }
                }
            }

            return true;
        }

        private void AddVersionToAbortSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            if (!this.abortSet.ContainsKey(tableId))
            {
                this.abortSet[tableId] = new Dictionary<object, List<PostProcessingEntry>>();
            }

            if (!this.abortSet[tableId].ContainsKey(recordKey))
            {
                this.abortSet[tableId][recordKey] = new List<PostProcessingEntry>(Transaction.POSTPROCESSING_LIST_MAX_CAPACITY);
            }
            this.abortSet[tableId][recordKey].Add(new PostProcessingEntry(versionKey, beginTs, endTs));
        }

        private void AddVersionToCommitSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            if (!this.commitSet.ContainsKey(tableId))
            {
                this.commitSet[tableId] = new Dictionary<object, List<PostProcessingEntry>>();
            }

            if (!this.commitSet[tableId].ContainsKey(recordKey))
            {
                this.commitSet[tableId][recordKey] = new List<PostProcessingEntry>(Transaction.POSTPROCESSING_LIST_MAX_CAPACITY);
            }
            this.commitSet[tableId][recordKey].Add(new PostProcessingEntry(versionKey, beginTs, endTs));
        }

        internal bool GetCommitTimestamp()
        {
            // CommitTs >= tx.CommitLowerBound
            // CommitTs >= entry.BeginTimestamp for read-only entry
            // CommitTs >= entry.BeginTimestamp + 1 for update entry
            // CommitTs >= tx.maxCommitTsOfWrites + 1

            long proposedCommitTs = this.maxCommitTsOfWrites + 1;

            foreach (string tableId in this.readSet.Keys)
            {
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    ReadSetEntry entry = this.readSet[tableId][recordKey];
                    if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
                    {
                        proposedCommitTs = Math.Max(proposedCommitTs, entry.BeginTimestamp + 1);
                    }
                    else
                    {
                        proposedCommitTs = Math.Max(proposedCommitTs, entry.BeginTimestamp);
                    }
                }
            }

            this.commitTs = this.versionDb.SetAndGetCommitTime(this.txId, proposedCommitTs);
            return this.commitTs != TxTableEntry.DEFAULT_COMMIT_TIME;
        }

        internal bool Validate()
        {
            //List<Tuple<string, VersionEntry>> validationVersions = new List<Tuple<string, VersionEntry>>();
            
            foreach (string tableId in this.readSet.Keys)
            {
                // List<VersionPrimaryKey> validationKeys = new List<VersionPrimaryKey>();
                foreach (object recordKey in this.readSet[tableId].Keys)
                {
                    // Validates records only in the read set but not in the write set.
                    // Records in the write set are already held on the current tx. 
                    if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
                    {
                        continue;
                    }

                    VersionEntry rereadEntry = this.versionDb.GetVersionEntryByKey(
                        tableId, recordKey, readSet[tableId][recordKey].VersionKey);

                    this.validationVersions.Add(Tuple.Create(tableId, recordKey, rereadEntry));

                    // validationKeys.Add(new VersionPrimaryKey(recordKey, readSet[tableId][recordKey].VersionKey));
                }

                //IDictionary<VersionPrimaryKey, VersionEntry> versionDict = 
                //    this.versionDb.GetVersionEntryByKey(tableId, validationKeys);

                //foreach (VersionEntry entry in versionDict.Values)
                //{
                //    validationVersions.Add(Tuple.Create(tableId, entry));
                //}
            }

            foreach (Tuple<string, object, VersionEntry> entry in this.validationVersions)
            {
                //first get the version, compare its maxCommitTs with my CommitTs
                VersionEntry versionEntry = entry.Item3;

                if (versionEntry.MaxCommitTs < this.CommitTs)
                {
					//we need to update the version's maxCommitTs
					// Step1: UpdateMaxCommitTime
					// try to push the version’s maxCommitTs as my CommitTs
					versionEntry = this.versionDb.UpdateVersionMaxCommitTs(
						entry.Item1,
						entry.Item2,
						versionEntry.VersionKey,
						this.commitTs);
				}

				// The current version entry has been held by another concurrent transaction
				if (versionEntry.TxId != VersionEntry.EMPTY_TXID)
				{
					// Step2: 
					// check the concurrent transaction's status and whether it has gotten its commit timestamp.
					// (1) if its status is Aborted, keep going
					// (2) if it has got its commit timestamp (its status is Committed or Ongoing), range check
					// (3) if it has not gotten its commit timestamp (its status must be Ongoing), try to push its commitLowerBound
					TxTableEntry txEntry = this.versionDb.GetTxTableEntry(versionEntry.TxId);
					if (txEntry.Status == TxStatus.Aborted)
					{
						continue;
					}
					else if (txEntry.CommitTime != TxTableEntry.DEFAULT_COMMIT_TIME)
					{
						if (this.commitTs > txEntry.CommitTime)
						{
							return false;
						}
					}
					else
					{
						// Step 3:
						// try to push the tx who is locking this version's commitLowerBound to myCommitTs + 1
						// if the tx has already got the commit time, it may be in 3 cases:
						// (1) ongoing: if it will be Committed later, we want to perform range check to ensure that the 
						//              current transaction is still valid after tx commit; 
						//              if it will be Aborted later, the current transaction must be valid
						//
						// (2) commited: just check the range
						// (3) aborted: the current transaction must be valid
						//
						// In conclusion, to simplify the process, in all the 3 cases we will only check the range, which may increase the abort rate
						//
						// Range check Rules:
						// (1) if my commitTs > tx's commitTime, abort
						// (2) if my commitTs < tx's commitTime, keep going
						long txCommitTs = this.versionDb.UpdateCommitLowerBound(versionEntry.TxId, this.commitTs + 1);
						if (txCommitTs == VersionDb.RETURN_ERROR_CODE)
						{
							return false;
						}
						// push the commit lower bound successfully
						else if (txCommitTs == TxTableEntry.DEFAULT_COMMIT_TIME)
						{
							//the tx who is locking the version has not gotten its commitTs and I push its commitLowerBound successfully.
							continue;
						}
						// push failed, the transaction holding the version entry has gotten the commit time
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
				}
				else
				{
					//range check
					if (this.commitTs > versionEntry.EndTimestamp)
					{
						return false;
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
                    foreach (PostProcessingEntry entry in this.abortSet[tableId][recordKey])
                    {
                        if (entry.BeginTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP)
                        {
                            //this is a new version inserted by this aborted tx, delete it
                            this.versionDb.DeleteVersionEntry(tableId, recordKey, entry.VersionKey);
                        }
                        else
                        {
                            this.versionDb.ReplaceVersionEntry(
                                tableId,
                                recordKey,
                                entry.VersionKey,
                                entry.BeginTimestamp,
                                entry.EndTimestamp,
                                VersionEntry.EMPTY_TXID,
                                this.txId,
                                long.MaxValue);
                        }
                    }
                }
            }

			if (this.garbageQueueTxId != null)
			{
				this.garbageQueueTxId.Enqueue(this.txId);
                this.garbageQueueFinishTime.Enqueue(DateTime.Now.Ticks);
			}
        }

        internal void PostProcessingAfterCommit()
        {
            foreach (string tableId in this.commitSet.Keys)
            {
                foreach (object recordKey in this.commitSet[tableId].Keys)
                {
                    foreach (PostProcessingEntry entry in this.commitSet[tableId][recordKey])
                    {
                        if (entry.BeginTimestamp == Transaction.UNSET_TX_COMMIT_TIMESTAMP)
                        {
                            //this is a new version inserted by this committed tx, try to change it
                            this.versionDb.ReplaceVersionEntry(
                                tableId,
                                recordKey,
                                entry.VersionKey,
                                this.commitTs,
                                entry.EndTimestamp,
                                VersionEntry.EMPTY_TXID,
                                this.txId,
                                -1);
                        }
                        else
                        {
                            if (this.versionDb is RedisVersionDb)
                            {
                                // Pass the whole version, need only 1 redis command.
                                // Note that we set the current transaction's commitTs as the old version's maxCommitTs.

                                ReadSetEntry readEntry = this.readSet[tableId][recordKey];
                                this.versionDb.ReplaceWholeVersionEntry(
                                    tableId,
                                    recordKey,
                                    entry.VersionKey,
                                    new VersionEntry(
                                        entry.VersionKey,
                                        readEntry.BeginTimestamp,
                                        this.commitTs,
                                        readEntry.Record,
                                        VersionEntry.EMPTY_TXID,
                                        this.commitTs));
                            }
                            else
                            {
                                // Just replaces the begin, end and txId fields in post-processing
                                this.versionDb.ReplaceVersionEntry(
                                    tableId,
                                    recordKey,
                                    entry.VersionKey,
                                    entry.BeginTimestamp,
                                    this.commitTs,
                                    VersionEntry.EMPTY_TXID,
                                    this.txId,
                                    long.MaxValue);
                            }
                        }
                    }
                }
            }

            if (this.garbageQueueTxId != null)
            {
                this.garbageQueueTxId.Enqueue(this.txId);
                this.garbageQueueFinishTime.Enqueue(DateTime.Now.Ticks);
            }
        }

        public bool Commit()
        {
            // Phase 3: Uploading Phase
            if (!this.UploadLocalWriteRecords())
            {
                this.Abort();
                return false;
            }

            // Phase 4: Choosing Commit Timestamp Phase
            if (!this.GetCommitTimestamp())
            {
                this.Abort();
                return false;
            }

            // Phase 5: Validate Phase
            if (!this.Validate())
            {
                this.Abort();
                return false;
            }

            // Phase 6: Commit Phase, Write to log and change the transaction status
            if (!this.WriteChangeToLog())
            {
                this.Abort();
                return false;
            }
            this.txStatus = TxStatus.Committed;
            this.versionDb.UpdateTxStatus(this.txId, TxStatus.Committed);

            // Phase 7: PostProcessing Phase
            this.PostProcessingAfterCommit();
            return true;
        }

        private bool WriteChangeToLog()
        {
			// UNCOMMENT THE LOG PART NOW

			//foreach (string tableId in this.writeSet.Keys)
			//{
			//	foreach (object recordKey in this.writeSet[tableId].Keys)
			//	{
			//		if (!this.logStore.WriteCommittedVersion(
			//			tableId, recordKey, this.writeSet[tableId][recordKey], this.txId, this.commitTs))
			//		{
			//			return false;
			//		}
			//	}
			//}

			//if (!this.logStore.WriteCommittedTx(this.txId))
			//{
			//	return false;
			//}

			return true;
        }
    }

    /// <summary>
    /// It's the execution phase in the transaction's whole life, which includes several operations:
    /// Read, Update, Insert and Delete
    /// </summary>
    public partial class Transaction
    {
        public void Insert(string tableId, object recordKey, object record)
        {
            // check whether the record is already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) && 
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.Abort();
                    return;
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
                return;
            }

            //add the new record to local writeSet
            if (!this.writeSet.ContainsKey(tableId))
            {
                this.writeSet[tableId] = new Dictionary<object, object>();
            }

            this.writeSet[tableId][recordKey] = record;
        }

        /// <summary>
        /// Read a record by version key in the transaction
        /// It will read in the following orders:
        /// (1) local write set, in case that it already have a insertion or update before
        /// (2) local read set, in case there is already a read operation
        /// (3) the version list from storage, in case it's the first read
        /// </summary>
        /// <returns>A version entry or null</returns>
        public object Read(string tableId, object recordKey)
        {
            // try to find the record image in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                return this.writeSet[tableId][recordKey];
            }

            // try to find the obejct in the local readSet
            if (this.readSet.ContainsKey(tableId) && 
                this.readSet[tableId].ContainsKey(recordKey))
            {
                return this.readSet[tableId][recordKey].Record;
            }

            // try to get the object from DB
            IEnumerable<VersionEntry> versionList = this.versionDb.GetVersionList(tableId, recordKey);

            long largestVersionKey = 0;
            VersionEntry versionEntry = this.GetVisibleVersionEntry(versionList, out largestVersionKey);

            // Store the largest version key into the map to infer the version key for future new versions
            if (!this.largestVersionKeyMap.ContainsKey(tableId))
            {
                this.largestVersionKeyMap[tableId] = new Dictionary<object, long>();
            }
            this.largestVersionKeyMap[tableId][recordKey] = largestVersionKey;

            if (versionEntry == null)
            {
                return null;
            }

            // Add the record to local readSet
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
        /// Update a record by version key in the transaction
        /// It will update in the following orders:
        /// (1) local write set, in case that it already have a insertion or update before
        /// (2) local read set, in case it's the first update which must have a read before
        /// In other cases, it must have some errors, throw an exception
        /// </summary>
        public void Update(string tableId, object recordKey, object record)
        {
            // check whether the record is already exist in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.writeSet[tableId][recordKey] = record;
                }
                // It means the last modification operation is delete, update is invalid
                else
                {
                    this.Abort();
                    return;
                }
            }

            //check whether the record is already exist in the local readSet
            else if (this.readSet.ContainsKey(tableId) &&
                     this.readSet[tableId].ContainsKey(recordKey))
            { 
                ReadSetEntry entry = this.readSet[tableId][recordKey];

                // this version is updatable only if its endTs or -1
                // Four types of readable version entry:
                // 1. [Ts, Inf, -1]: UPDATEABLE
                // 2. [Ts, Inf, TxId]: NOT UPDATEABLE, w-w confilct, it will be aborted in the validation phase
                // 3. [-1, -1, TxId]: UPDATEABLE, commited version without postprocessing
                // 4. [Ts, Ts', -1]: NOT UPDATEABLE, not the most recent committed version,

                // It will be dropped
                // if (entry.EndTimestamp != long.MaxValue && entry.EndTimestamp != VersionEntry.DEFAULT_END_TIMESTAMP)
                // {
                //     this.Abort();
                //     throw new TransactionException("Update");
                // }

                // add the update record to the local writeSet
                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, object>();
                }

                this.writeSet[tableId][recordKey] = record;
            }
            else
            {
                this.Abort();
                return;
            }
        }

        /// <summary>
        /// Delete a record by version key in the transaction
        /// It will delete in the following orders:
        /// (1) local write set, in case that it already have a insertion or update before
        /// (2) local read set, in case it's the first update which must have a read before
        /// In other cases, it must have some errors, throw an exception
        /// </summary>
        public void Delete(string tableId, object recordKey)
        {
            // check whether the record already exists in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.writeSet[tableId][recordKey] = null;
                }
                // It means the last modification is deletion, the current deletion is invalid
                else
                {
                    this.Abort();
                    return;
                }
            }
            //check whether the record is already exist in the local readSet
            else if (this.readSet.ContainsKey(tableId) &&
                this.readSet[tableId].ContainsKey(recordKey))
            {
                // this version is deleteable only if its endTs or -1
                // Four types of readable version entry:
                // 1. [Ts, Inf, -1]: DELETEABLE
                // 2. [Ts, Inf, TxId]: UNDELETEABLE, w-w confilct, it will be aborted in the validation phase
                // 3. [-1, -1, TxId]: DELETEABLE, commited version without postprocessing
                // 4. [Ts, Ts', -1]: UNDELETEABLE, not the most recent committed version
                ReadSetEntry entry = this.readSet[tableId][recordKey];

                // It will be dropped
                // if (entry.EndTimestamp != long.MaxValue && entry.EndTimestamp != VersionEntry.DEFAULT_END_TIMESTAMP)
                // {
                //     this.Abort();
                //     throw new TransactionException("Delete");
                // }

                // add the delete record to the local writeSet
                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, object>();
                }
                this.writeSet[tableId][recordKey] = null;
            }
            else
            {
                this.Abort();
                return;
            }
        }

        /// <summary>
        /// Read a record by version key in the transaction and initialize the list if the version list is empty
        /// It will read in the following orders:
        /// (1) local write set, in case that it already have a insertion or update before
        /// (2) local read set, in case there is already a read operation
        /// (3) the version list from storage, in case it's the first read
        /// </summary>
        /// <returns>A version entry or null</returns>
        public object ReadAndInitialize(string tableId, object recordKey)
        {
            // try to find the object in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                return this.writeSet[tableId][recordKey];
            }

            // try to find the obejct in the local readSet
            if (this.readSet.ContainsKey(tableId) && 
                this.readSet[tableId].ContainsKey(recordKey))
            {
                return this.readSet[tableId][recordKey].Record;
            }

            // try to get the objects from DB
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

            if (versionList == null)
            {
                return null;
            }

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

                    if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP)
                    {
                        if (pendingTxStatus == TxStatus.Ongoing || pendingTxStatus == TxStatus.Aborted)
                        {
                            continue;
                        }
                    }
                }

                if (visibleVersion == null)
                {
                    if (versionEntry.TxId == VersionEntry.EMPTY_TXID)
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
                        else if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP && pendingTxStatus == TxStatus.Committed)
                        {
                            visibleVersion = new VersionEntry(
                                versionEntry.VersionKey, 
                                pendingTxEntry.CommitTime, 
                                long.MaxValue, 
                                versionEntry.Record, 
                                VersionEntry.EMPTY_TXID, 
                                versionEntry.MaxCommitTs);
                        }
                    }
                }

                largestVersionKey = Math.Max(largestVersionKey, versionEntry.VersionKey);
            }

            return visibleVersion;
        }
    }
}

