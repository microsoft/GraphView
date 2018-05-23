
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Diagnostics;

    internal delegate void Procedure();

    internal enum TxProgress
    {
        Read,
        Insert,
        Update,
        Delete,
        Open,
        Initi,
        Final,
        Close,
    }

    class TxAbortReasonTracer
    {
        public static string[] reasons;  // = new NewOrderState[10];
    }

    internal class TransactionExecution
    {
		public bool DEBUG_MODE = false;

        internal static readonly long DEFAULT_TX_BEGIN_TIMESTAMP = -1L;

        internal static readonly int POSTPROCESSING_LIST_MAX_CAPACITY = 2;

        internal static readonly long UNSET_TX_COMMIT_TIMESTAMP = -2L;

        internal static readonly int TX_REQUEST_GARBAGE_QUEUE_SIZE = 30;

        private readonly ILogStore logStore;

        private readonly VersionDb versionDb;

        internal long txId;

        private Stack<TxRequest> requestStack;

        internal long commitTs;

        internal long maxCommitTsOfWrites;

        internal long beginTimestamp;

        internal bool isCommitted;

        internal Dictionary<string, Dictionary<object, ReadSetEntry>> readSet;

        internal Dictionary<string, Dictionary<object, object>> writeSet;

        internal List<PostProcessingEntry> abortSet;

        internal List<PostProcessingEntry> commitSet;

        internal Dictionary<string, Dictionary<object, long>> largestVersionKeyMap;


        /// <summary>
        /// The list resource in the execution phase
        /// If it ends the current procedure normally, those list resource will be recycled at 
        /// the end of every phase. 
        /// 
        /// If it will go to abort during any phase, those list resource will be recycled at
        /// the end of every phase normally. 
        /// </summary>
        // The tableIdList will be shared in different phase
        private List<string> tableIdList;

        private List<object> recordKeyList;

        // The readVersionEntry method and validation method share a read version list,
        // Since they are in different stage, there won't conflicts
        private List<VersionEntry> readVersionList;

        //private readonly Queue<Tuple<long, long>> garbageQueue;
        private readonly Queue<long> garbageQueueTxId;
        private readonly Queue<long> garbageQueueFinishTime;

        /// <summary>
        /// A garbage queue for tx requests
        /// all tx requests will be enqueued in the current execution and will be 
        /// recycled at the end of postprocessing phase
        /// </summary>
        private readonly Queue<TxRequest> txReqGarbageQueue;

        private readonly TxRange txRange;

        private readonly TransactionExecutor executor;

        private long beginTicks;

        internal Procedure CurrentProc { get; private set; }

        internal TxProgress Progress { get; private set; }

        internal StoredProcedure Procedure { get; private set; }

        private Procedure validateProc;
        private Procedure abortPostproProc;
        private Procedure uploadProc;
        private UploadVersionRequest uploadReq;
        private ReplaceVersionRequest replaceReq;
        private ReplaceVersionRequest retryReplaceReq;
        private GetTxEntryRequest getTxReq;
        private Procedure abortProc;

        private NewTxIdRequest newTxIdReq;
        private Procedure newTxIdProc;
        private InsertTxIdRequest inserTxIdReq;
        private Procedure insertTxIdProc;
        private RecycleTxRequest recycleTxReq;
        private Procedure recycleTxProc;
        private SetCommitTsRequest commitTsReq;
        private Procedure commitTsProc;
        private UpdateTxStatusRequest updateTxReq;
        private Procedure updateTxProc;
        private List<ReplaceVersionRequest> replaceReqList = new List<ReplaceVersionRequest>();
        private Procedure commitVersionProc;

        internal long ExecutionSeconds
        {
            get
            {
                long elapsedTicks = DateTime.Now.Ticks - this.beginTicks;
                return elapsedTicks / 10000000;
            }
        }

        public TransactionExecution(
            ILogStore logStore,
            VersionDb versionDb,
            StoredProcedure procedure = null,
            Queue<long> garbageQueueTxId = null,
            Queue<long> garbageQueueFinishTime = null,
            TxRange txRange = null, 
            TransactionExecutor executor = null)
        {
            this.logStore = logStore;
            this.versionDb = versionDb;
            this.readSet = new Dictionary<string, Dictionary<object, ReadSetEntry>>();
            this.writeSet = new Dictionary<string, Dictionary<object, object>>();
            this.abortSet = new List<PostProcessingEntry>();
            this.commitSet = new List<PostProcessingEntry>();
            this.largestVersionKeyMap = new Dictionary<string, Dictionary<object, long>>();
            this.garbageQueueTxId = garbageQueueTxId;
            this.garbageQueueFinishTime = garbageQueueFinishTime;
            this.txRange = txRange;
            this.executor = executor;

            this.txReqGarbageQueue = new Queue<TxRequest>(TX_REQUEST_GARBAGE_QUEUE_SIZE);

            this.validateProc = new Procedure(this.Validate);
            this.uploadProc = new Procedure(this.Upload);
            this.abortPostproProc = new Procedure(this.PostProcessingAfterAbort);
            this.abortProc = new Procedure(this.Abort);
            this.newTxIdProc = new Procedure(this.NewTxId);
            this.newTxIdProc = new Procedure(this.NewTxId);
            this.recycleTxProc = new Procedure(this.RecycleTx);
            this.commitTsProc = new Procedure(this.FinalizeCommitTs);
            this.updateTxProc = new Procedure(this.UpdateTxStatus);
            this.commitVersionProc = new Procedure(this.CommitModifications);

            this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.maxCommitTsOfWrites = -1L;
            this.beginTimestamp = TransactionExecution.DEFAULT_TX_BEGIN_TIMESTAMP;

            this.Progress = TxProgress.Open;
            this.requestStack = new Stack<TxRequest>();

            this.Procedure = procedure;

            this.isCommitted = false;

            this.beginTicks = DateTime.Now.Ticks;

            // init and get tx id
            this.InitTx();
        }

        internal void Reset(StoredProcedure procedure = null)
        {
            foreach (Dictionary<object, ReadSetEntry> versionList in this.readSet.Values)
            {
                versionList.Clear();
            }
            // It will not clear the whole readSet, writeSet, abortSet, commitSet, largestVersionKeymap
            // Since we assume that the tableId is always same, there is no need to clear them and append again

            foreach (Dictionary<object, object> writeRecords in this.writeSet.Values)
            {
                writeRecords.Clear();
            }

            this.abortSet.Clear();
            this.commitSet.Clear();

            foreach (Dictionary<object, long> keymap in this.largestVersionKeyMap.Values)
            {
                keymap.Clear();
            }

            this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.maxCommitTsOfWrites = -1L;
            this.beginTimestamp = TransactionExecution.DEFAULT_TX_BEGIN_TIMESTAMP;

            this.Progress = TxProgress.Open;
            this.requestStack.Clear();

            this.Procedure = procedure;
            this.isCommitted = false;
            this.beginTicks = DateTime.Now.Ticks;

            this.txReqGarbageQueue.Clear();

            this.uploadReq = null;
            this.replaceReq = null;
            this.retryReplaceReq = null;
            this.getTxReq = null;
            this.newTxIdReq = null;
            this.inserTxIdReq = null;
            this.recycleTxReq = null;
            this.commitTsReq = null;
            this.updateTxReq = null;
            
            // reset the list as null
            this.readVersionList = null;
            this.tableIdList = null;
            this.recordKeyList = null;
            this.replaceReqList.Clear();

            // init and get tx id
            this.InitTx();
        }

        private void PopulateWriteKeyList()
        {
            this.tableIdList = this.executor.ResourceManager.GetTableIdList();
            this.recordKeyList = this.executor.ResourceManager.GetRecordKeyList();
            foreach (string tableId in this.writeSet.Keys)
            {
                foreach (object recordKey in this.writeSet[tableId].Keys)
                {
                    this.tableIdList.Add(tableId);
                    this.recordKeyList.Add(recordKey);
                }
            }
        }

        internal void SetAbortMsg(string msg)
        {
            //TxAbortReasonTracer.reasons[this.Procedure.pid] = msg;
        }

        

        internal void InitTx()
        {
            this.Progress = TxProgress.Initi;
            long candidateId = -1;

            if (this.garbageQueueTxId != null && this.garbageQueueTxId.Count > 0)
            {
                long candidate = this.garbageQueueTxId.Peek();
                long finishTime = this.garbageQueueFinishTime.Peek();

                if (DateTime.Now.Ticks - finishTime >= TransactionExecutor.elapsed)
                {
                    this.garbageQueueTxId.Dequeue();
                    this.garbageQueueFinishTime.Dequeue();

                    this.recycleTxReq = this.executor.ResourceManager.RecycleTxRequest(candidate);
                    this.txReqGarbageQueue.Enqueue(this.recycleTxReq);
                    this.versionDb.EnqueueTxEntryRequest(candidate, this.recycleTxReq);

                    this.CurrentProc = this.recycleTxProc;
                    this.RecycleTx();
                    return;
                }
            }

            long id = this.txRange.NextTxCandidate();
            this.newTxIdReq = this.executor.ResourceManager.NewTxIdRequest(id);
            this.txReqGarbageQueue.Enqueue(this.newTxIdReq);
            this.versionDb.EnqueueTxEntryRequest(id, this.newTxIdReq);

            this.CurrentProc = this.newTxIdProc;
            this.NewTxId();
        }

        internal void NewTxId()
        {
            if (!this.newTxIdReq.Finished)
            {
                return;
            }

            while ((long)this.newTxIdReq.Result == 0)
            {
                // Retry in loop to get the unique txId
                long newId = this.txRange.NextTxCandidate();
                NewTxIdRequest retryReq = this.executor.ResourceManager.NewTxIdRequest(newId);
                this.txReqGarbageQueue.Enqueue(retryReq);
                this.versionDb.EnqueueTxEntryRequest(newId, retryReq);
                this.newTxIdReq = retryReq;

                if (this.newTxIdReq.Finished)
                {
                    continue;
                }
                else
                {
                    return;
                }
            }

            this.txId = this.newTxIdReq.TxId;
            this.newTxIdReq = null;

            this.inserTxIdReq = this.executor.ResourceManager.InsertTxRequest(this.txId);
            this.txReqGarbageQueue.Enqueue(this.inserTxIdReq);
            this.versionDb.EnqueueTxEntryRequest(this.txId, this.inserTxIdReq);

            this.CurrentProc = this.insertTxIdProc;
            this.InsertTxId();
        }

        internal void InsertTxId()
        {
            if (!this.inserTxIdReq.Finished)
            {
                return;
            }

            // Assume the tx is always inserted successfully. 
            this.inserTxIdReq = null;
            this.CurrentProc = null;
            this.Progress = TxProgress.Open;
        }

        internal void RecycleTx()
        {
            if (!this.recycleTxReq.Finished)
            {
                return;
            }

            if ((long)this.recycleTxReq.Result == 0)
            {
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
            }

            // Recycled successfully
            this.txId = this.recycleTxReq.TxId;
            this.recycleTxReq = null;
            this.CurrentProc = null;
            this.Progress = TxProgress.Open;
        }

        internal void Upload()
        {
            this.Progress = TxProgress.Final;

            if (this.recordKeyList == null)
            {
                this.PopulateWriteKeyList();
            }

            while (this.recordKeyList.Count > 0)
            {
                // Prior write set entry has been uploaded successfully
                if (this.replaceReq == null && 
                    this.uploadReq == null && 
                    this.getTxReq == null && 
                    this.retryReplaceReq == null)     
                {
                    Debug.Assert(this.recordKeyList.Count > 0);
                    int lastIndex = this.recordKeyList.Count - 1;

                    string tableId = this.tableIdList[lastIndex];
                    this.tableIdList.RemoveAt(lastIndex);
                    object recordKey = this.recordKeyList[lastIndex];
                    this.recordKeyList.RemoveAt(lastIndex);

                    object payload = this.writeSet[tableId][recordKey];

                    // should check the type of writes, insert/update/delete
                    // The write-set record is an insert or update record, try to insert the new version
                    if (payload != null)
                    {
                        VersionEntry newImageEntry = new VersionEntry(
                                recordKey,
                                this.largestVersionKeyMap[tableId][recordKey] + 1,
                                payload,
                                this.txId);
                        
                        this.uploadReq = this.executor.ResourceManager.
                            UploadVersionRequest(tableId, recordKey, newImageEntry.VersionKey, newImageEntry);
                        this.txReqGarbageQueue.Enqueue(this.uploadReq);
                        this.versionDb.EnqueueVersionEntryRequest(tableId, this.uploadReq);
                    }
                    // The write-set record is an delete record, only replace the old version
                    else
                    {
                        ReadSetEntry readVersion = this.readSet[tableId][recordKey];
                        this.replaceReq = this.executor.ResourceManager.ReplaceVersionRequest(
                            tableId,
                            recordKey,
                            readVersion.VersionKey,
                            readVersion.BeginTimestamp,
                            long.MaxValue,
                            this.txId,
                            VersionEntry.EMPTY_TXID,
                            long.MaxValue);

                        this.txReqGarbageQueue.Enqueue(this.replaceReq);
                        this.versionDb.EnqueueVersionEntryRequest(tableId, this.replaceReq);
                    }
                }
                else if (this.uploadReq != null && this.replaceReq == null && 
                    this.retryReplaceReq == null && this.getTxReq == null)
                {
                    if (!this.uploadReq.Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    bool uploadSuccess = this.uploadReq.Result == null ? false : Convert.ToBoolean(uploadReq.Result);
                    if (!uploadSuccess)
                    {
                        // Failed to upload the new image. Moves to the abort phase.
                        this.SetAbortMsg("Failed to upload the new image");
						// Failed to upload the new image. Moves to the abort phase.
						this.CurrentProc = this.abortProc;

						if (!this.DEBUG_MODE)
						{
                            this.Abort();
						}

                        return;
                    }

                    string tableId = uploadReq.TableId;
                    object recordKey = uploadReq.RecordKey;

                    // Add the info to the abortSet
                    this.AddVersionToAbortSet(tableId, recordKey, this.uploadReq.VersionKey,
                        VersionEntry.DEFAULT_BEGIN_TIMESTAMP, VersionEntry.DEFAULT_END_TIMESTAMP);
                    // Add the info to the commitSet
                    this.AddVersionToCommitSet(tableId, recordKey, this.uploadReq.VersionKey,
                        TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP, long.MaxValue);

                    this.uploadReq = null;

                    if (!this.readSet.ContainsKey(tableId) || !this.readSet[tableId].ContainsKey(recordKey))
                    {
                        // The write-set record is an insert record. 
                        // Moves to the next write-set record or the next phase.
                        continue;
                    }

                    ReadSetEntry readVersion = this.readSet[tableId][recordKey];
                    // Initiates a new request to append the new image to the tail of the version list.
                    // The tail entry could be [Ts, inf, -1], [Ts, inf, txId1] or [-1, -1, txId1].
                    // The first case indicates that no concurrent tx is locking the tail.
                    // The second case indicates that one concurrent tx is holding the tail. 
                    // The third case means that a concurrent tx is creating a new tail, which was seen by this tx. 
                    this.replaceReq = this.executor.ResourceManager.ReplaceVersionRequest(
                        tableId,
                        recordKey,
                        readVersion.VersionKey,
                        readVersion.BeginTimestamp,
                        long.MaxValue,
                        this.txId,
                        VersionEntry.EMPTY_TXID,
                        long.MaxValue);

                    this.txReqGarbageQueue.Enqueue(this.replaceReq);
                    this.versionDb.EnqueueVersionEntryRequest(tableId, this.replaceReq);
                }
                else if (this.replaceReq != null && this.uploadReq == null &&
                    this.retryReplaceReq == null && this.getTxReq == null)
                {
                    if (!this.replaceReq.Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    VersionEntry versionEntry = this.replaceReq.Result as VersionEntry;
                    if (versionEntry == null)
                    {
                        this.SetAbortMsg("Version Entry null");
						this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
                            this.Abort();
						}

                        return;
                    }

                    long rolledBackBegin = versionEntry.BeginTimestamp;
                    this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, versionEntry.MaxCommitTs);

                    if (versionEntry.TxId == this.txId) // Appending to the tail was successful
                    {
                        // Add the updated tail to the abort set
                        this.AddVersionToAbortSet(
                            replaceReq.TableId,
                            replaceReq.RecordKey,
                            replaceReq.VersionKey,
                            rolledBackBegin,
                            long.MaxValue);
                        // Add the updated tail to the commit set
                        this.AddVersionToCommitSet(
                            replaceReq.TableId,
                            replaceReq.RecordKey,
                            replaceReq.VersionKey,
                            rolledBackBegin,
                            TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP);

                        // Move on to the next write-set record or the next phase
                        continue;
                    }
                    else if (versionEntry.TxId >= 0)
                    {
                        // The first try was unsuccessful because the tail is hold by another concurrent tx. 
                        // If the concurrent tx has finished (committed or aborted), there is a chance for this tx
                        // to re-gain the lock. 
                        // Enqueues a request to check the status of the tx that is holding the tail.
                        this.getTxReq = this.executor.ResourceManager.GetTxEntryRequest(versionEntry.TxId);
                        this.txReqGarbageQueue.Enqueue(this.getTxReq);
                        this.versionDb.EnqueueTxEntryRequest(versionEntry.TxId, this.getTxReq);

                        continue;
                    }
                    else
                    {
                        // The new version is failed to append to the tail of the version list, 
                        // because the old tail seen by this tx is not the tail anymore

                        this.SetAbortMsg("Failed to append the tail version");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.Abort();
						}
                        return;
                    }
                }
                else if (this.replaceReq != null && this.getTxReq != null &&
                    this.retryReplaceReq == null && this.uploadReq == null)
                {
                    if (!this.getTxReq.Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    TxTableEntry conflictTxStatus = this.getTxReq.Result as TxTableEntry;
                    this.getTxReq = null;

                    if (conflictTxStatus == null || conflictTxStatus.Status == TxStatus.Ongoing)
                    {
                        this.SetAbortMsg("conflict tx status Ongoing");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.Abort();
						}

                        return;
                    }

                    VersionEntry versionEntry = this.replaceReq.Result as VersionEntry;
                    
                    // The new tail was created by a concurrent tx, yet has not been post-processed. 
                    // The current tx tries to update the tail to the post-processing image and obtain the lock.
                    if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP)
                    {
                        // Only if a new tail's owner tx has been committed, can it be seen by 
                        // the current tx, who is trying to gain the lock of the tail. 
                        Debug.Assert(conflictTxStatus.Status == TxStatus.Committed);

                        this.retryReplaceReq = this.executor.ResourceManager.ReplaceVersionRequest(
                            replaceReq.TableId,
                            replaceReq.RecordKey,
                            versionEntry.VersionKey,
                            conflictTxStatus.CommitTime,
                            long.MaxValue,
                            this.txId,
                            conflictTxStatus.TxId,
                            VersionEntry.DEFAULT_END_TIMESTAMP);

                        this.txReqGarbageQueue.Enqueue(this.retryReplaceReq);
                        this.versionDb.EnqueueVersionEntryRequest(replaceReq.TableId, this.retryReplaceReq);
                    }
                    // The old tail was locked by a concurrent tx, which has finished but has not released the lock. 
                    // The current lock tries to replace the lock's owner to itself. 
                    else if (versionEntry.EndTimestamp == long.MaxValue)
                    {
                        // The owner tx of the lock has committed. This version entry is not the tail anymore.
                        if (conflictTxStatus.Status == TxStatus.Committed)
                        {
                            this.SetAbortMsg("the owner tx of the lock committed");
                            this.CurrentProc = this.abortProc;
							if (!this.DEBUG_MODE)
							{
								this.Abort();
							}
                            return;
                        }
                        else if (conflictTxStatus.Status == TxStatus.Aborted)
                        {
                            this.retryReplaceReq = this.executor.ResourceManager.ReplaceVersionRequest(
                                replaceReq.TableId,
                                replaceReq.RecordKey,
                                versionEntry.VersionKey,
                                versionEntry.BeginTimestamp,
                                long.MaxValue,
                                this.txId,
                                conflictTxStatus.TxId,
                                long.MaxValue);

                            this.txReqGarbageQueue.Enqueue(this.retryReplaceReq);
                            this.versionDb.EnqueueVersionEntryRequest(replaceReq.TableId, this.retryReplaceReq);
                        }
                    }
                }
                else if (this.replaceReq != null && this.retryReplaceReq != null && 
                    this.uploadReq == null && this.getTxReq == null)
                {
                    if (!this.retryReplaceReq.Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    VersionEntry retryEntry = this.retryReplaceReq.Result as VersionEntry;
                    if (retryEntry == null || retryEntry.TxId != this.txId)
                    {
                        this.SetAbortMsg("retry entry null...");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
                            this.Abort();
						}
                        return;
                    }

                    long rolledBackBegin = retryEntry.BeginTimestamp;
                    this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, retryEntry.MaxCommitTs);

                    // Add the updated tail to the abort set
                    this.AddVersionToAbortSet(
                        this.retryReplaceReq.TableId, 
                        this.retryReplaceReq.RecordKey, 
                        this.retryReplaceReq.VersionKey,
                        rolledBackBegin, 
                        long.MaxValue);

                    // Add the updated tail to the commit set
                    this.AddVersionToCommitSet(
                        this.retryReplaceReq.TableId, 
                        this.retryReplaceReq.RecordKey, 
                        this.retryReplaceReq.VersionKey,
                        rolledBackBegin, 
                        TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP);

                    this.replaceReq = null;
                    this.retryReplaceReq = null;
                }
                else
                {
                    throw new TransactionException("An illegal state of the uploading phase.");
                }
            }

            // Move on to the next phase
            this.executor.ResourceManager.RecycleTableIdList(ref this.tableIdList);
            this.executor.ResourceManager.RecycleRecordKeyList(ref this.recordKeyList);

            this.SetCommitTimestamp();
        }

        private void AddVersionToAbortSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            PostProcessingEntry abortEntry = this.executor.ResourceManager.GetPostProcessingEntry();
            abortEntry.TableId = tableId;
            abortEntry.RecordKey = recordKey;
            abortEntry.VersionKey = versionKey;
            abortEntry.BeginTimestamp = beginTs;
            abortEntry.EndTimestamp = endTs;
            this.abortSet.Add(abortEntry);
        }

        private void AddVersionToCommitSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            PostProcessingEntry commitEntry = this.executor.ResourceManager.GetPostProcessingEntry();
            commitEntry.TableId = tableId;
            commitEntry.RecordKey = recordKey;
            commitEntry.VersionKey = versionKey;
            commitEntry.BeginTimestamp = beginTs;
            commitEntry.EndTimestamp = endTs;
            this.commitSet.Add(commitEntry);
        }

        internal void SetCommitTimestamp()
        {
            Debug.Assert(this.commitTs < 0);

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

            this.commitTs = proposedCommitTs;

            this.commitTsReq = this.executor.ResourceManager.SetCommitTsRequest(this.txId, this.commitTs);
            this.txReqGarbageQueue.Enqueue(this.commitTsReq);
            this.versionDb.EnqueueTxEntryRequest(this.txId, this.commitTsReq);
            this.CurrentProc = this.commitTsProc;
            this.FinalizeCommitTs();
        }

        internal void FinalizeCommitTs()
        {
            if (!this.commitTsReq.Finished)
            {
                // The prior request hasn't been processed. Returns the control to the caller.
                return;
            }

            long commitTime = this.commitTsReq.Result == null ? -1 : (long)this.commitTsReq.Result;
            if (commitTime < 0)
            {
                this.SetAbortMsg("commit time < 0");
                this.CurrentProc = this.abortProc;
                if (!this.DEBUG_MODE)
                {
                    this.CurrentProc();
                }
                return;
            }
            else
            {
                this.commitTs = commitTime;
                this.CurrentProc = this.validateProc;
                if (!this.DEBUG_MODE)
                {
                    this.CurrentProc();
                }
                return;
            }
        }

        internal void Validate()
        {
            // Have sent the GetVersionEntry request, but not received the response yet
            if (this.tableIdList == null)
            {
                if (this.requestStack.Count == 0)
                {
                    // Enqueues all requests re-reading the version to be validated 
                    foreach (string tableId in this.readSet.Keys)
                    {
                        foreach (object recordKey in this.readSet[tableId].Keys)
                        {
                            if (this.writeSet.ContainsKey(tableId) && this.writeSet[tableId].ContainsKey(recordKey))
                            {
                                continue;
                            }

                            ReadVersionRequest readReq = this.executor.ResourceManager.ReadVersionRequest(
                                tableId, recordKey, readSet[tableId][recordKey].VersionKey);
                            this.versionDb.EnqueueVersionEntryRequest(tableId, readReq);
                            this.txReqGarbageQueue.Enqueue(readReq);
                            this.requestStack.Push(readReq);
                        }
                    }
                }

                foreach (TxRequest req in this.requestStack)
                {
                    // If any of the re-reading ops has not finished, returns the control to the caller.
                    if (!req.Finished)
                    {
                        return;
                    }
                }

                // All re-reading ops have finished
                this.tableIdList = this.executor.ResourceManager.GetTableIdList();
                this.readVersionList = this.executor.ResourceManager.GetVersionList();
                foreach (TxRequest req in this.requestStack)
                {
                    ReadVersionRequest readVersionReq = req as ReadVersionRequest;
                    VersionEntry readEntry = req.Result as VersionEntry;
                    if (readEntry == null)
                    {
                        this.SetAbortMsg("read entry null");

                        // A really serious bug, should clear the stack before enter the next step
                        this.requestStack.Clear();
                        this.CurrentProc = this.abortProc;

						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}
                        return;
                    }

                    this.tableIdList.Add(readVersionReq.TableId);
                    this.readVersionList.Add(readEntry);
                }

                this.requestStack.Clear();
            }

            // Already re-read the version entries back, need to check visiable
            while (this.tableIdList.Count > 0 || this.requestStack.Count > 0)
            {
                int lastIndex = this.tableIdList.Count - 1;
                // No concurrent txs hold the version or already received the response
                if (this.requestStack.Count == 0)
                {
                    // validateKeyList and readVersionList both have the same size, they share a lastIndex variable
                    string tableId = this.tableIdList[lastIndex];
                    VersionEntry readVersion = this.readVersionList[lastIndex];

                    if (readVersion.MaxCommitTs >= this.commitTs)
                    {
                        // No need to update the version's maxCommitTs.
                        // Check whether or not the re-read version is occupied by another tx.
                        if (readVersion.TxId != VersionEntry.EMPTY_TXID)
                        {
                            // A concurrent tx is locking the version. Checks the tx's status to decide how to move forward, 
                            // i.e., abort or pass validation.
                            GetTxEntryRequest getTxReq = this.executor.ResourceManager.GetTxEntryRequest(readVersion.TxId);
                            this.versionDb.EnqueueTxEntryRequest(readVersion.TxId, getTxReq);
                            this.txReqGarbageQueue.Enqueue(getTxReq);
                            this.requestStack.Push(getTxReq);
                        }
                        else
                        {
                            if (this.commitTs > readVersion.EndTimestamp)
                            {
                                // A new version has been created before this tx can commit.
                                // Abort the tx.
                                this.SetAbortMsg("a new version has been created before this commit");
                                this.CurrentProc = this.abortProc;
								if (!this.DEBUG_MODE)
								{
									this.CurrentProc();
								}
                                return;
                            }
                            else
                            {
                                // No new version has bee created. This record passes validation. 
                                // remove the last item will be an O(1) operation
                                this.tableIdList.RemoveAt(lastIndex);
                                this.readVersionList.RemoveAt(lastIndex);
                                continue;
                            }
                        }
                    }
                    else
                    {
                        // Updates the version's max commit timestamp
                        UpdateVersionMaxCommitTsRequest updateMaxTsReq = this.executor.ResourceManager.UpdateVersionMaxCommitTsRequest(
                            tableId, readVersion.RecordKey, readVersion.VersionKey, this.commitTs);
                        this.versionDb.EnqueueVersionEntryRequest(tableId, updateMaxTsReq);
                        this.txReqGarbageQueue.Enqueue(updateMaxTsReq);
                        this.requestStack.Push(updateMaxTsReq);
                    }
                }
                // Try to push the version's maxCommitTimestamp if necessary
                else if (this.requestStack.Count == 1 && this.requestStack.Peek() is UpdateVersionMaxCommitTsRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        // The pending request hasn't been processed. Returns the control to the caller.
                        return;
                    }
                    
                    UpdateVersionMaxCommitTsRequest updateMaxTsReq = this.requestStack.Pop() as UpdateVersionMaxCommitTsRequest;
                    VersionEntry readEntry = updateMaxTsReq.Result as VersionEntry;
                    if (readEntry == null)
                    {
                        this.SetAbortMsg("read entry null: update Max Ts Req");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}
                        return;
                    }

                    // Successfully updated the version's max commit ts. 
                    // Replaces the old version entry by the new one.
                    // REALLY SMART HERE

                    //Tuple<string, VersionEntry> valTuple = this.validateKeyList.Pop();
                    //this.validateKeyList.Push(Tuple.Create(valTuple.Item1, readEntry));
                    this.readVersionList.RemoveAt(lastIndex);
                    this.readVersionList.Add(readEntry);

                    continue;
                }
                // The re-read version entry is holden by a concurrent tx and we already got tx's state
                // we should check the commit status and commit time to determine whether to push tx's lowerBound
                else if (this.requestStack.Count == 1 && this.requestStack.Peek() is GetTxEntryRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        // The pending request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    GetTxEntryRequest getTxReq = this.requestStack.Pop() as GetTxEntryRequest;
                    TxTableEntry txEntry = getTxReq.Result as TxTableEntry;
                    if (txEntry == null)
                    {
                        this.SetAbortMsg("tx table entry null");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc(); 
						}
                        return;
                    }

                    if (txEntry.Status == TxStatus.Aborted)
                    {
                        // The tx holding the version has been aborted. Validation passed.
                        this.tableIdList.RemoveAt(lastIndex);
                        this.readVersionList.RemoveAt(lastIndex);
                        continue;
                    }
                    else if (txEntry.Status == TxStatus.Committed || txEntry.CommitTime >= 0)
                    {
                        if (this.commitTs > txEntry.CommitTime)
                        {
                            this.SetAbortMsg("this.commitTs > txEntry.CommitTime");
                            this.CurrentProc = this.abortProc;
							if (!this.DEBUG_MODE)
							{
								this.CurrentProc();
							}
                            return;
                        }
                        else
                        {
                            // pass the validation
                            this.tableIdList.RemoveAt(lastIndex);
                            this.readVersionList.RemoveAt(lastIndex);
                            continue;
                        }
                    }
                    else
                    {
                        UpdateCommitLowerBoundRequest updateCommitBoundReq =
                            this.executor.ResourceManager.UpdateCommitLowerBound(txEntry.TxId, this.commitTs + 1);
                        this.versionDb.EnqueueTxEntryRequest(txEntry.TxId, updateCommitBoundReq);
                        this.txReqGarbageQueue.Enqueue(updateCommitBoundReq);
                        this.requestStack.Push(updateCommitBoundReq);
                    }
                }
                // The re-read version entry is holden by a concurrent tx and we received the reponse of pushing commit lower bound
                else if (this.requestStack.Count == 1 && this.requestStack.Peek() is UpdateCommitLowerBoundRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        // The pending request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    UpdateCommitLowerBoundRequest txCommitReq = this.requestStack.Pop() as UpdateCommitLowerBoundRequest;
                    long txCommitTs = txCommitReq.Result == null ? VersionDb.RETURN_ERROR_CODE : (long)txCommitReq.Result;

					if (txCommitTs == VersionDb.RETURN_ERROR_CODE)
					{
                        this.SetAbortMsg("txCommitTs == VersionDb.RETURN_ERROR_CODE");
						this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}
                        return;
                    }
                    else if (txCommitTs == TxTableEntry.DEFAULT_COMMIT_TIME)
                    {
                        // The tx who is locking the version has not had its commit timestamp.
                        this.tableIdList.RemoveAt(lastIndex);
                        this.readVersionList.RemoveAt(lastIndex);
                        continue;
                    }
                    else if (this.commitTs > txCommitTs)
                    {
                        this.SetAbortMsg("this.commitTs > txCommitTs");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}
                        return;
                    }
                    else if (this.commitTs <= txCommitTs)
                    {
                        // pass the validation
                        this.tableIdList.RemoveAt(lastIndex);
                        this.readVersionList.RemoveAt(lastIndex);
                        continue;
                    }
                }
                //else
                //{
                //    throw new TransactionException("An illegal state of tx validation.");
                //}
            }

            // All versions pass validation. Move to the commit phase.
            this.executor.ResourceManager.RecycleVersionList(ref this.readVersionList);
            this.executor.ResourceManager.RecycleTableIdList(ref this.tableIdList);

            this.WriteToLog();
        }

        internal void WriteToLog()
        {
            this.updateTxReq = this.executor.ResourceManager.UpdateTxStatusRequest(this.txId, TxStatus.Committed);
            this.txReqGarbageQueue.Enqueue(this.updateTxReq);
            this.versionDb.EnqueueTxEntryRequest(this.txId, this.updateTxReq);
            this.CurrentProc = this.updateTxProc;
            this.UpdateTxStatus();
        }

        internal void UpdateTxStatus()
        {
            if (!this.updateTxReq.Finished)
            {
                return;
            }

            this.updateTxReq = null;
            this.isCommitted = true;
            this.PostProcessingAfterCommit();
        }

        internal void PostProcessingAfterCommit()
        {
            this.replaceReqList.Clear();

            for (int iter = 0; iter < this.commitSet.Count; iter++)
            {
                PostProcessingEntry entry = this.commitSet[iter];
                if (entry.BeginTimestamp == TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP)
                {
                    ReplaceVersionRequest replaceVerReq = this.executor.ResourceManager.ReplaceVersionRequest(
                        entry.TableId,
                        entry.RecordKey,
                        entry.VersionKey,
                        this.commitTs,
                        entry.EndTimestamp,
                        VersionEntry.EMPTY_TXID,
                        this.txId,
                        -1);

                    this.txReqGarbageQueue.Enqueue(replaceVerReq);
                    this.versionDb.EnqueueVersionEntryRequest(entry.TableId, replaceVerReq);
                    this.replaceReqList.Add(replaceVerReq);
                }
                else
                {
                    ReplaceVersionRequest replaceVerReq = this.executor.ResourceManager.ReplaceVersionRequest(
                        entry.TableId,
                        entry.RecordKey,
                        entry.VersionKey,
                        entry.BeginTimestamp,
                        this.commitTs,
                        VersionEntry.EMPTY_TXID,
                        this.txId,
                        long.MaxValue);

                    this.txReqGarbageQueue.Enqueue(replaceVerReq);
                    this.versionDb.EnqueueVersionEntryRequest(entry.TableId, replaceVerReq);
                    this.replaceReqList.Add(replaceVerReq);

                    //// Single machine setting: pass the whole version, need only 1 redis command.
                    //ReadSetEntry readEntry = this.readSet[entry.TableId][entry.RecordKey];
                    //ReplaceWholeVersionRequest replaceWholeVerReq = this.executor.ResourceManager.ReplaceWholeVersionRequest(
                    //    entry.TableId,
                    //    entry.RecordKey,
                    //    entry.VersionKey,
                    //    new VersionEntry(
                    //        entry.RecordKey,
                    //        entry.VersionKey,
                    //        readEntry.BeginTimestamp,
                    //        this.commitTs,
                    //        readEntry.Record,
                    //        VersionEntry.EMPTY_TXID,
                    //        this.commitTs));

                    //this.versionDb.EnqueueVersionEntryRequest(entry.TableId, replaceWholeVerReq);
                    //this.txReqGarbageQueue.Enqueue(replaceWholeVerReq);
                    //this.requestStack.Push(replaceWholeVerReq);
                }

                this.executor.ResourceManager.RecyclePostProcessingEntry(ref entry);
            }

            if (this.replaceReqList.Count == 0)
            {
                this.Progress = TxProgress.Close;
                this.CurrentProc = null;
                if (this.garbageQueueTxId != null)
                {
                    this.garbageQueueTxId.Enqueue(this.txId);
                    this.garbageQueueFinishTime.Enqueue(DateTime.Now.Ticks);
                }

                while (this.txReqGarbageQueue.Count > 0)
                {
                    TxRequest req = this.txReqGarbageQueue.Dequeue();
                    this.executor.ResourceManager.RecycleTxRequest(ref req);
                }

                return;
            }
            else
            {
                this.CurrentProc = this.commitVersionProc;
                this.CommitModifications();
            }
        }

        internal void CommitModifications()
        {
            foreach (TxRequest req in this.replaceReqList)
            {
                if (!req.Finished)
                {
                    return;
                }
            }

            while (this.txReqGarbageQueue.Count > 0)
            {
                TxRequest req = this.txReqGarbageQueue.Dequeue();
                this.executor.ResourceManager.RecycleTxRequest(ref req);
            }

            // All post-processing records have been uploaded.
            this.replaceReqList.Clear();
            this.Progress = TxProgress.Close;
            this.CurrentProc = null;
            if (this.garbageQueueTxId != null)
            {
                this.garbageQueueTxId.Enqueue(this.txId);
                this.garbageQueueFinishTime.Enqueue(DateTime.Now.Ticks);
            }
            return;
        }

        internal void PostProcessingAfterAbort()
        {
            if (this.requestStack.Count == 0)
            {
                for (int iter = 0; iter < this.abortSet.Count; iter++)
                {
                    PostProcessingEntry entry = this.abortSet[iter];
                    if (entry.BeginTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP)
                    {
                        DeleteVersionRequest delVerReq = this.executor.ResourceManager.DeleteVersionRequest(
                            entry.TableId, entry.RecordKey, entry.VersionKey);
                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, delVerReq);
                        this.txReqGarbageQueue.Enqueue(delVerReq);
                        this.requestStack.Push(delVerReq);
                    }
                    else
                    {
                        ReplaceVersionRequest replaceVerReq = this.executor.ResourceManager.ReplaceVersionRequest(
                            entry.TableId,
                            entry.RecordKey,
                            entry.VersionKey,
                            entry.BeginTimestamp,
                            entry.EndTimestamp,
                            VersionEntry.EMPTY_TXID,
                            this.txId,
                            long.MaxValue);
                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, replaceVerReq);
                        this.txReqGarbageQueue.Enqueue(replaceVerReq);
                        this.requestStack.Push(replaceVerReq);
                    }
                    this.executor.ResourceManager.RecyclePostProcessingEntry(ref entry);
                }

				// No post processing ops are needed.
				if (this.requestStack.Count == 0)
				{
					this.Progress = TxProgress.Close;
					this.CurrentProc = null;
                    if (this.garbageQueueTxId != null)
                    {
                        this.garbageQueueTxId.Enqueue(this.txId);
                        this.garbageQueueFinishTime.Enqueue(DateTime.Now.Ticks);
                    }
                }
                return;
			}
            else
            {
                foreach (TxRequest req in this.requestStack)
                {
                    if (!req.Finished)
                    {
                        return;
                    }
                }

                while (this.txReqGarbageQueue.Count > 0)
                {
                    TxRequest req = this.txReqGarbageQueue.Dequeue();
                    this.executor.ResourceManager.RecycleTxRequest(ref req);
                }

                while (this.txReqGarbageQueue.Count > 0)
                {
                    TxRequest req = this.txReqGarbageQueue.Dequeue();
                    this.executor.ResourceManager.RecycleTxRequest(ref req);
                }

                // All pending records have been reverted.
                this.Progress = TxProgress.Close;
                this.CurrentProc = null;
                if (this.garbageQueueTxId != null)
                {
                    this.garbageQueueTxId.Enqueue(this.txId);
                    this.garbageQueueFinishTime.Enqueue(DateTime.Now.Ticks);
                }
                return;
            }
        }

        /// <summary>
        /// Abort if timeout
        /// </summary>
        internal void TimeoutAbort()
        {
            this.requestStack.Clear();
            this.SetAbortMsg("time out abort");
            this.CurrentProc = this.abortProc;
        }

        internal void Abort()
        {
            // Retry to recycle the list resources
            this.RecycleContainerResource();

            this.Progress = TxProgress.Final;

            if (this.requestStack.Count == 0)
            {
                UpdateTxStatusRequest updateTxReq = this.executor.ResourceManager.UpdateTxStatusRequest(this.txId, TxStatus.Aborted);
                this.versionDb.EnqueueTxEntryRequest(this.txId, updateTxReq);
                this.txReqGarbageQueue.Enqueue(updateTxReq);
                this.requestStack.Push(updateTxReq);
                return;
            }
            else if (this.requestStack.Count == 1 && this.requestStack.Peek() is UpdateTxStatusRequest)
            {
                if (!this.requestStack.Peek().Finished)
                {
                    return;
                }

                this.requestStack.Pop();
				this.CurrentProc = this.abortPostproProc;
				if (!this.DEBUG_MODE)
				{
					this.CurrentProc();
				}
				return;
            }
        }

        public void Insert(string tableId, object recordKey, object record)
        {
            // Checks whether the record is in the local write set
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.SetAbortMsg("write set tableid recordkey null");
                    this.CurrentProc = this.abortProc;
                    this.CurrentProc();
                    //throw new TransactionException("Cannot insert the same record key twice.");
                }
                else
                {
                    this.writeSet[tableId][recordKey] = record;
                    return;
                }
            }

            // Checks whether the record is already in the local read set
            if (this.readSet.ContainsKey(tableId) &&
                this.readSet[tableId].ContainsKey(recordKey))
            {
                this.SetAbortMsg("record is already in the local read set");
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
                //throw new TransactionException("The same record already exists.");
            }

            // Add the new record to local writeSet
            if (!this.writeSet.ContainsKey(tableId))
            {
                this.writeSet[tableId] = new Dictionary<object, object>();
            }

            this.writeSet[tableId][recordKey] = record;
        }

        public void Read(string tableId, object recordKey, out bool received, out object payload)
        {
            this.Read(tableId, recordKey, false, out received, out payload);
        }

        public void Update(string tableId, object recordKey, object payload)
        {
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    this.writeSet[tableId][recordKey] = payload;
                }
                // The record has been deleted by this tx. Cannot be updated. 
                else
                {
                    this.SetAbortMsg("record has been deleted by this tx");
                    this.CurrentProc = this.abortProc;
                    this.CurrentProc();
                    //throw new TransactionException("The record to be updated has been deleted.");
                }
            }
            else if (this.readSet.ContainsKey(tableId) &&
                     this.readSet[tableId].ContainsKey(recordKey))
            {
                ReadSetEntry entry = this.readSet[tableId][recordKey];

                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, object>();
                }

                this.writeSet[tableId][recordKey] = payload;
            }
            else
            {
                this.SetAbortMsg("update fail, some reason");
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
                //throw new TransactionException("The record has not been read or does not exist. Cannot update it.");
            }
        }

        public void Delete(string tableId, object recordKey, out object payload)
        {
            payload = null;
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                if (this.writeSet[tableId][recordKey] != null)
                {
                    payload = this.writeSet[tableId][recordKey];
                    this.writeSet[tableId][recordKey] = null;
                }
                else
                {
                    this.SetAbortMsg("delete fail reason1");
                    this.CurrentProc = this.abortProc;
                    this.CurrentProc();
                    // throw new TransactionException("The record to be deleted has been deleted by the same tx.");
                }
            }
            else if (this.readSet.ContainsKey(tableId) &&
                this.readSet[tableId].ContainsKey(recordKey))
            {
                payload = this.readSet[tableId][recordKey].Record;

                if (!this.writeSet.ContainsKey(tableId))
                {
                    this.writeSet[tableId] = new Dictionary<object, object>();
                }
                this.writeSet[tableId][recordKey] = null;
            }
            else
            {
                this.SetAbortMsg("delete fail reason2");
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
                // throw new TransactionException("The record has not been read or does not exist. Cannot delete it.");
            }
        }

        public void ReadAndInitialize(string tableId, object recordKey, out bool received, out object payload)
        {
            this.Read(tableId, recordKey, true, out received, out payload);
        }

        private void Read(string tableId, object recordKey, bool initi, out bool received, out object payload)
        {
            received = false;
            payload = null;
            // In those two cases, the read process is non-blocked, so keep the progress as OPEN
            // try to find the record image in the local writeSet
            if (this.writeSet.ContainsKey(tableId) &&
                this.writeSet[tableId].ContainsKey(recordKey))
            {
                payload = this.writeSet[tableId][recordKey];
                received = true;
                return;
            }

            // try to find the obejct in the local readSet
            if (this.readSet.ContainsKey(tableId) &&
                this.readSet[tableId].ContainsKey(recordKey))
            {
                payload =  this.readSet[tableId][recordKey].Record;
                received = true;
                return;
            }

            // if the version entry would be read is not in the local version list,
            // the tx should send requests to get version list from storage, set the Progress as READ 
            // to prevent other operations.
            this.Progress = TxProgress.Read;

            // Have not got the version list from GetVersionList request yet.
            while (this.readVersionList == null)
            {
                // Have not sent the GetVersionListRequest
                if (this.requestStack.Count == 0)
                {
                    List<VersionEntry> container = this.executor.ResourceManager.GetVersionList();

                    if (initi)
                    {
                        InitiGetVersionListRequest initiGetVersionListReq = 
                            this.executor.ResourceManager.GetInitiGetVersionListRequest(tableId, recordKey);
                        initiGetVersionListReq.Container = container;

                        this.versionDb.EnqueueVersionEntryRequest(tableId, initiGetVersionListReq);
                        this.txReqGarbageQueue.Enqueue(initiGetVersionListReq);
                        this.requestStack.Push(initiGetVersionListReq);
                    }
                    else
                    {
                        GetVersionListRequest getVlistReq =
                            this.executor.ResourceManager.GetVersionListRequest(tableId, recordKey);
                        getVlistReq.Container = container;

                        this.versionDb.EnqueueVersionEntryRequest(tableId, getVlistReq);
                        this.txReqGarbageQueue.Enqueue(getVlistReq);
                        this.requestStack.Push(getVlistReq);
                    }
                }
                // The reqeust have been sent and now waits for response from GetVersionList request
                else if (this.requestStack.Count == 1 && this.requestStack.Peek() is GetVersionListRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        return;
                    }

                    GetVersionListRequest getVersionListReq = this.requestStack.Pop() as GetVersionListRequest;
                    this.readVersionList = getVersionListReq.Result as List<VersionEntry>;

                    if (this.readVersionList == null)
                    {
                        received = true;
                        payload = null;
                        this.Progress = TxProgress.Open;
                        return;
                    }

                    // Sort the version list by the descending order of version keys.
                    this.readVersionList.Sort();
                    break;
                }
                else if (this.requestStack.Count == 1 && this.requestStack.Peek() is InitiGetVersionListRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        return;
                    }

                    InitiGetVersionListRequest initReq = this.requestStack.Pop() as InitiGetVersionListRequest;
                    // The current version list is empty and initilized
                    if ((long)initReq.Result == 1)
                    {
                        received = true;
                        payload = null;
                        if (!this.largestVersionKeyMap.ContainsKey(tableId))
                        {
                            this.largestVersionKeyMap[tableId] = new Dictionary<object, long>();
                        }
                        this.largestVersionKeyMap[tableId][recordKey] = VersionEntry.VERSION_KEY_STRAT_INDEX;
                        VersionEntry emptyEntry = VersionEntry.InitEmptyVersionEntry(initReq.RecordKey);

                        this.readVersionList = initReq.Container;
                        this.readVersionList.Add(emptyEntry);
                        // break to keep finishing the remained steps
                        break;
                    }
                    else
                    {
                        List<VersionEntry> container = this.executor.ResourceManager.GetVersionList();
                        GetVersionListRequest getVlistReq = 
                            this.executor.ResourceManager.GetVersionListRequest(tableId, recordKey);
                        getVlistReq.Container = container;
                            
                        this.versionDb.EnqueueVersionEntryRequest(tableId, getVlistReq);
                        this.txReqGarbageQueue.Enqueue(getVlistReq);
                        this.requestStack.Push(getVlistReq);
                    }
                }
                //else
                //{
                //    throw new TransactionException("An illegal state of tx read");
                //}
            }

            VersionEntry visibleVersion = null;
            // Keep a committed version to retrieve the largest version key
            VersionEntry committedVersion = null;
            while (this.readVersionList.Count > 0)
            {
                int lastIndex = this.readVersionList.Count - 1;
                // Wait for the GetTxEntry response
                if (this.requestStack.Count == 1 && this.requestStack.Peek() is GetTxEntryRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        return;
                    }

                    GetTxEntryRequest getTxReq = this.requestStack.Pop() as GetTxEntryRequest;
                    TxTableEntry pendingTx = getTxReq.Result as TxTableEntry;

                    if (pendingTx == null)
                    {
                        // Failed to retrieve the status of the tx holding the version. 
                        // Moves on to the next version.
                        this.readVersionList.RemoveAt(lastIndex);
                        continue;
                    }

                    // The last version entry is the one need to check whether visiable
                    VersionEntry versionEntry = this.readVersionList[this.readVersionList.Count - 1];
                    this.readVersionList.RemoveAt(lastIndex);

                    // If the version entry is a dirty write, skips the entry.
                    if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP && 
                        (pendingTx.Status == TxStatus.Ongoing || pendingTx.Status == TxStatus.Aborted))
                    {
                        continue;
                    }

                    // The current version is commited and should be extracted the largest version key
                    committedVersion = versionEntry;

                    // A dirty write has been appended after this version entry. 
                    // This version is visible if the writing tx has not been committed 
                    if (versionEntry.EndTimestamp == long.MaxValue && pendingTx.Status != TxStatus.Committed)
                    {
                        visibleVersion = versionEntry;
                    }
                    // A dirty write is visible to this tx when the writing tx has been committed, 
                    // which has not finished postprocessing and changing the dirty write to a normal version entry
                    else if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP && pendingTx.Status == TxStatus.Committed)
                    {
                        visibleVersion = new VersionEntry(
                            versionEntry.RecordKey,
                            versionEntry.VersionKey,
                            pendingTx.CommitTime,
                            long.MaxValue,
                            versionEntry.Record,
                            VersionEntry.EMPTY_TXID,
                            versionEntry.MaxCommitTs);
                    }
                }
                else if (this.requestStack.Count == 0)
                {
                    VersionEntry versionEntry = this.readVersionList[this.readVersionList.Count - 1];

                    if (versionEntry.TxId >= 0)
                    {
                        // Send the GetTxEntry request
                        GetTxEntryRequest getTxReq = 
                            this.executor.ResourceManager.GetTxEntryRequest(versionEntry.TxId);

                        this.versionDb.EnqueueTxEntryRequest(versionEntry.TxId, getTxReq);
                        this.txReqGarbageQueue.Enqueue(getTxReq);
                        this.requestStack.Push(getTxReq);
                    }
                    else
                    {
                        committedVersion = versionEntry;
                        // When a tx has a begin timestamp after intialization
                        if (this.beginTimestamp >= 0 && 
                            this.beginTimestamp >= versionEntry.BeginTimestamp && 
                            this.beginTimestamp < versionEntry.EndTimestamp)
                        {
                            
                            visibleVersion = versionEntry;
                        }
                        // When a tx has no begin timestamp after intialization, the tx is under serializability. 
                        // A read always returns the most-recently committed version.
                        else if (versionEntry.EndTimestamp == long.MaxValue)
                        {
                            visibleVersion = versionEntry;
                        }
                        else
                        {
                            this.readVersionList.RemoveAt(lastIndex);
                        }
                    }
                }
                //else
                //{
                //    throw new TransactionException("An illegal state of tx read.");
                //}

                // Retrieve the largest version key from commit version entry
                if (!this.largestVersionKeyMap.ContainsKey(tableId))
                {
                    this.largestVersionKeyMap[tableId] = new Dictionary<object, long>();
                }
                if (!this.largestVersionKeyMap[tableId].ContainsKey(recordKey) && committedVersion != null)
                {
                    this.largestVersionKeyMap[tableId][recordKey] = committedVersion.VersionKey;
                }

                // Break the loop once find a visiable version
                if (visibleVersion != null)
                {
                    break;
                }
            }

            if (visibleVersion != null)
            {
                payload = visibleVersion.Record;

                // Add the record to local readSet
                if (!this.readSet.ContainsKey(tableId))
                {
                    this.readSet[tableId] = new Dictionary<object, ReadSetEntry>();
                }

                this.readSet[tableId][recordKey] = new ReadSetEntry(
                    visibleVersion.VersionKey,
                    visibleVersion.BeginTimestamp,
                    visibleVersion.EndTimestamp,
                    visibleVersion.TxId,
                    visibleVersion.Record);
            }

            // reset read version list
            this.executor.ResourceManager.RecycleVersionList(ref this.readVersionList);
            this.Progress = TxProgress.Open;
            received = true;
        }

        public void Commit()
        {
            Debug.Assert(this.Progress == TxProgress.Open);

            this.CurrentProc = this.uploadProc;
            this.CurrentProc();
            //this.WriteToLog();
        }

        private void RecycleContainerResource()
        {
            if (this.readVersionList != null)
            {
                this.executor.ResourceManager.RecycleVersionList(ref this.readVersionList);
            }

            if (this.tableIdList != null)
            {
                this.executor.ResourceManager.RecycleTableIdList(ref this.tableIdList);
            }

            if (this.recordKeyList != null)
            {
                this.executor.ResourceManager.RecycleRecordKeyList(ref this.recordKeyList);
            }
        }
    }
}
