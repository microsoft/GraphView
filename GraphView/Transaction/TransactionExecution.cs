
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

        internal Dictionary<string, Dictionary<object, List<PostProcessingEntry>>> abortSet;

        internal Dictionary<string, Dictionary<object, List<PostProcessingEntry>>> commitSet;

        internal Dictionary<string, Dictionary<object, long>> largestVersionKeyMap;

        private Queue<Tuple<string, object>> writeKeyList;

        private List<string> validateKeyList;

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
        private Procedure setCommitTsProc;
        private Procedure commitPostproProc;
        private Procedure abortPostproProc;
        private Procedure uploadProc;
        private Procedure initiTxProc;
        private Procedure abortProc;

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
            this.abortSet = new Dictionary<string, Dictionary<object, List<PostProcessingEntry>>>();
            this.commitSet = new Dictionary<string, Dictionary<object, List<PostProcessingEntry>>>();
            this.largestVersionKeyMap = new Dictionary<string, Dictionary<object, long>>();
            this.garbageQueueTxId = garbageQueueTxId;
            this.garbageQueueFinishTime = garbageQueueFinishTime;
            this.txRange = txRange;
            this.executor = executor;

            this.txReqGarbageQueue = new Queue<TxRequest>(TX_REQUEST_GARBAGE_QUEUE_SIZE);

            this.validateProc = new Procedure(this.Validate);
            this.uploadProc = new Procedure(this.Upload);
            this.setCommitTsProc = new Procedure(this.SetCommitTimestamp);
            this.abortPostproProc = new Procedure(this.PostProcessingAfterAbort);
            this.commitPostproProc = new Procedure(this.PostProcessingAfterCommit);
            this.initiTxProc = new Procedure(this.InitTx);
            this.abortProc = new Procedure(this.Abort);

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
            this.readSet.Clear();

            foreach (Dictionary<object, object> writeRecords in this.writeSet.Values)
            {
                writeRecords.Clear();
            }
            this.writeSet.Clear();

            foreach (Dictionary<object, List<PostProcessingEntry>> abortEntries in this.abortSet.Values)
            {
                abortEntries.Clear();
            }
            this.abortSet.Clear();

            foreach (Dictionary<object, List<PostProcessingEntry>> commitEntries in this.commitSet.Values)
            {
                commitEntries.Clear();
            }
            this.commitSet.Clear();

            foreach (Dictionary<object, long> keymap in this.largestVersionKeyMap.Values)
            {
                keymap.Clear();
            }
            this.largestVersionKeyMap.Clear();

            this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.maxCommitTsOfWrites = -1L;
            this.beginTimestamp = TransactionExecution.DEFAULT_TX_BEGIN_TIMESTAMP;

            this.Progress = TxProgress.Open;
            this.requestStack.Clear();

            this.Procedure = procedure;
            this.isCommitted = false;
            this.beginTicks = DateTime.Now.Ticks;

            this.txReqGarbageQueue.Clear();

            // reset the list as null
            this.readVersionList = null;
            this.validateKeyList = null;

            // init and get tx id
            this.InitTx();
        }

        private void PopulateWriteKeyList()
        {
            this.writeKeyList = new Queue<Tuple<string, object>>();
            foreach (string tableId in this.writeSet.Keys)
            {
                foreach (object recordKey in this.writeSet[tableId].Keys)
                {
                    this.writeKeyList.Enqueue(new Tuple<string, object>(tableId, recordKey));
                }
            }
        }

        internal void SetAbortMsg(string msg)
        {
            //TxAbortReasonTracer.reasons[this.Procedure.pid] = msg;
        }

        internal void InitTx()
        {
            // Haven't sent the request
            if (this.requestStack.Count == 0)
            {
                long candidateId = -1;
                if (this.garbageQueueTxId != null && this.garbageQueueTxId.Count > 0)
                {
                    long candidate = this.garbageQueueTxId.Peek();
                    long finishTime = this.garbageQueueFinishTime.Peek();

                    if (DateTime.Now.Ticks - finishTime >= TransactionExecutor.elapsed)
                    {
                        RecycleTxRequest recycleReq = this.executor.ResourceManager.RecycleTxRequest(candidate);
                        this.versionDb.EnqueueTxEntryRequest(candidate, recycleReq);
                        this.txReqGarbageQueue.Enqueue(recycleReq);
                        this.requestStack.Push(recycleReq);
                    }
                }
                else
                {
                    NewTxIdRequest newTxIdReq = null;
                    if (this.txRange == null)
                    {
                        // TODO: should handle this case?
                        newTxIdReq = this.versionDb.EnqueueNewTxId();
                    }
                    else
                    {
                        long id = this.txRange.NextTxCandidate();
                        newTxIdReq = this.executor.ResourceManager.NewTxIdRequest(id);
                        this.versionDb.EnqueueTxEntryRequest(id, newTxIdReq);
                        this.txReqGarbageQueue.Enqueue(newTxIdReq);
                    }

                    this.requestStack.Push(newTxIdReq);
                }

                // set the current procedure as InitTx and wait for the executor check
                this.CurrentProc = this.initiTxProc;
                this.Progress = TxProgress.Initi;
                return;
            }
            else if (this.requestStack.Count == 1 && this.requestStack.Peek() is NewTxIdRequest)
            {
                if (!this.requestStack.Peek().Finished)
                {
                    return;
                }

                NewTxIdRequest newTxIdReq = this.requestStack.Pop() as NewTxIdRequest;
                if ((long)newTxIdReq.Result == 0)
                {
                    // Retry in loop to get the unique txId
                    NewTxIdRequest retryReq = null;
                    if (this.txRange == null)
                    {
                        retryReq = this.versionDb.EnqueueNewTxId();
                    }
                    else
                    {
                        long id = this.txRange.NextTxCandidate();
                        retryReq = this.executor.ResourceManager.NewTxIdRequest(id);
                        this.versionDb.EnqueueTxEntryRequest(id, retryReq);
                        this.txReqGarbageQueue.Enqueue(retryReq);
                    }
                    this.requestStack.Push(retryReq);
                    return;
                }
                this.txId = newTxIdReq.TxId;

                InsertTxIdRequest insertTxReq = this.executor.ResourceManager.InsertTxRequest(this.txId);
                this.versionDb.EnqueueTxEntryRequest(this.txId, insertTxReq);
                this.txReqGarbageQueue.Enqueue(insertTxReq);
                this.requestStack.Push(insertTxReq);
                return;
            }
            else if (this.requestStack.Count == 1 && this.requestStack.Peek() is InsertTxIdRequest)
            {
                if (!this.requestStack.Peek().Finished)
                {
                    return;
                }

                InsertTxIdRequest insertTxReq = this.requestStack.Pop() as InsertTxIdRequest;
                if (insertTxReq == null)
                {
                    this.SetAbortMsg("Insert Tx Id request failed");
                    this.CurrentProc = this.abortProc;
                    this.CurrentProc();

                    return;
                }

                // Assume the tx has been inserted successfully, change the execution status
                this.CurrentProc = null;
                this.Progress = TxProgress.Open;
            }
            else if (this.requestStack.Count == 1 && this.requestStack.Peek() is RecycleTxRequest)
            {
                if (!this.requestStack.Peek().Finished)
                {
                    return;
                }

                RecycleTxRequest recycleReq = this.requestStack.Pop() as RecycleTxRequest;
                if (recycleReq == null || (long)recycleReq.Result == 0)
                {
                    // throw new TransactionException("Recycling tx Id failed.");
                    this.requestStack.Clear();
                    this.CurrentProc = this.abortProc;
                    this.CurrentProc();
                }

				// Recycled successfully
				this.txId = recycleReq.TxId;
                this.garbageQueueTxId.Dequeue();
                this.garbageQueueFinishTime.Dequeue();
                this.CurrentProc = null;
                this.Progress = TxProgress.Open;
            }
        }

        internal void Upload()
        {
            this.Progress = TxProgress.Final;

            if (this.writeKeyList == null)
            {
                this.PopulateWriteKeyList();
            }

            while (this.writeKeyList.Count > 0 || this.requestStack.Count > 0)
            {
                if (requestStack.Count == 0)     // Prior write set entry has been uploaded successfully
                {
                    Debug.Assert(writeKeyList.Count > 0);

                    Tuple<string, object> writeTuple = writeKeyList.Dequeue();

                    string tableId = writeTuple.Item1;
                    object recordKey = writeTuple.Item2;
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

                        
                        UploadVersionRequest txRequest = this.executor.ResourceManager.
                            UploadVersionRequest(tableId, recordKey, newImageEntry.VersionKey, newImageEntry);
                        this.versionDb.EnqueueVersionEntryRequest(tableId, txRequest);
                        this.txReqGarbageQueue.Enqueue(txRequest);
                        this.requestStack.Push(txRequest);
                        return;
                    }
                    // The write-set record is an delete record, only replace the old version
                    else
                    {
                        ReadSetEntry readVersion = this.readSet[tableId][recordKey];
                        ReplaceVersionRequest replaceVerReq = this.executor.ResourceManager.ReplaceVersionRequest(
                            tableId,
                            recordKey,
                            readVersion.VersionKey,
                            readVersion.BeginTimestamp,
                            long.MaxValue,
                            this.txId,
                            VersionEntry.EMPTY_TXID,
                            long.MaxValue);
                        this.versionDb.EnqueueVersionEntryRequest(tableId, replaceVerReq);
                        this.txReqGarbageQueue.Enqueue(replaceVerReq);
                        this.requestStack.Push(replaceVerReq);
                        return;
                    }
                }
                else if (this.requestStack.Count == 1 && this.requestStack.Peek() is UploadVersionRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    UploadVersionRequest uploadReq = (this.requestStack.Pop()) as UploadVersionRequest;

                    bool uploadSuccess = uploadReq.Result == null ? false : Convert.ToBoolean(uploadReq.Result);
                    if (!uploadSuccess)
                    {
                        // Failed to upload the new image. Moves to the abort phase.
                        this.SetAbortMsg("Failed to upload the new image");
						// Failed to upload the new image. Moves to the abort phase.
						this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}

                        return;
                    }

                    string tableId = uploadReq.TableId;
                    object recordKey = uploadReq.RecordKey;

                    // Add the info to the abortSet
                    this.AddVersionToAbortSet(tableId, recordKey, uploadReq.VersionKey,
                        VersionEntry.DEFAULT_BEGIN_TIMESTAMP, VersionEntry.DEFAULT_END_TIMESTAMP);
                    // Add the info to the commitSet
                    this.AddVersionToCommitSet(tableId, recordKey, uploadReq.VersionKey,
                        TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP, long.MaxValue);

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
                    ReplaceVersionRequest replaceReq = this.executor.ResourceManager.ReplaceVersionRequest(
                        tableId,
                        recordKey,
                        readVersion.VersionKey,
                        readVersion.BeginTimestamp,
                        long.MaxValue,
                        this.txId,
                        VersionEntry.EMPTY_TXID,
                        long.MaxValue);
                    this.versionDb.EnqueueVersionEntryRequest(tableId, replaceReq);
                    this.txReqGarbageQueue.Enqueue(replaceReq);
                    this.requestStack.Push(replaceReq);
                    return;
                }
                else if (this.requestStack.Count == 1 && this.requestStack.Peek() is ReplaceVersionRequest)
                {
                    if (!requestStack.Peek().Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    ReplaceVersionRequest replaceReq = (requestStack.Pop()) as ReplaceVersionRequest;
                    VersionEntry versionEntry = replaceReq.Result as VersionEntry;
                    if (versionEntry == null)
                    {
                        this.SetAbortMsg("Version Entry null");
						this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
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
                        GetTxEntryRequest txStatusReq = this.executor.ResourceManager.GetTxEntryRequest(versionEntry.TxId);
                        this.versionDb.EnqueueTxEntryRequest(versionEntry.TxId, txStatusReq);
                        this.txReqGarbageQueue.Enqueue(txStatusReq);
                        this.requestStack.Push(replaceReq);
                        this.requestStack.Push(txStatusReq);
                        return;
                    }
                    else
                    {
                        // The new version is failed to append to the tail of the version list, 
                        // because the old tail seen by this tx is not the tail anymore

                        this.SetAbortMsg("Failed to append the tail version");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}
                        return;
                    }
                }
                else if (this.requestStack.Count == 2 && this.requestStack.Peek() is GetTxEntryRequest)
                {
                    if (!requestStack.Peek().Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    GetTxEntryRequest getTxReq = requestStack.Pop() as GetTxEntryRequest;
                    ReplaceVersionRequest replaceReq = requestStack.Pop() as ReplaceVersionRequest;

                    TxTableEntry conflictTxStatus = getTxReq.Result as TxTableEntry;

                    if (conflictTxStatus == null || conflictTxStatus.Status == TxStatus.Ongoing)
                    {
                        this.SetAbortMsg("conflict tx status Ongoing");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}

                        return;
                    }

                    VersionEntry versionEntry = replaceReq.Result as VersionEntry;
                    // The new tail was created by a concurrent tx, yet has not been post-processed. 
                    // The current tx tries to update the tail to the post-processing image and obtain the lock.
                    if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP)
                    {
                        // Only if a new tail's owner tx has been committed, can it be seen by 
                        // the current tx, who is trying to gain the lock of the tail. 
                        Debug.Assert(conflictTxStatus.Status == TxStatus.Committed);

                        ReplaceVersionRequest retryRequest = this.executor.ResourceManager.ReplaceVersionRequest(
                            replaceReq.TableId,
                            replaceReq.RecordKey,
                            versionEntry.VersionKey,
                            conflictTxStatus.CommitTime,
                            long.MaxValue,
                            this.txId,
                            conflictTxStatus.TxId,
                            VersionEntry.DEFAULT_END_TIMESTAMP);
                        this.versionDb.EnqueueVersionEntryRequest(replaceReq.TableId, retryRequest);
                        this.txReqGarbageQueue.Enqueue(retryRequest);
                        this.requestStack.Push(replaceReq);
                        this.requestStack.Push(retryRequest);
                        return;
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
								this.CurrentProc();
							}
                            return;
                        }
                        else if (conflictTxStatus.Status == TxStatus.Aborted)
                        {
                            ReplaceVersionRequest retryRequest = this.executor.ResourceManager.ReplaceVersionRequest(
                                replaceReq.TableId,
                                replaceReq.RecordKey,
                                versionEntry.VersionKey,
                                versionEntry.BeginTimestamp,
                                long.MaxValue,
                                this.txId,
                                conflictTxStatus.TxId,
                                long.MaxValue);

                            this.txReqGarbageQueue.Enqueue(retryRequest);
                            this.requestStack.Push(replaceReq);
                            this.requestStack.Push(retryRequest);
                            return;
                        }
                    }
                }
                else if (this.requestStack.Count == 2 && this.requestStack.Peek() is ReplaceVersionRequest)
                {
                    if (!this.requestStack.Peek().Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    ReplaceVersionRequest retryReq = this.requestStack.Pop() as ReplaceVersionRequest;
                    ReplaceVersionRequest originReq = this.requestStack.Pop() as ReplaceVersionRequest;

                    this.requestStack.Pop();

                    VersionEntry retryEntry = retryReq.Result as VersionEntry;
                    if (retryEntry == null || retryEntry.TxId != this.txId)
                    {
                        this.SetAbortMsg("retry entry null...");
                        this.CurrentProc = this.abortProc;
						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}
                        return;
                    }

                    long rolledBackBegin = retryEntry.BeginTimestamp;
                    this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, retryEntry.MaxCommitTs);

                    // Add the updated tail to the abort set
                    this.AddVersionToAbortSet(retryReq.TableId, retryReq.RecordKey, retryReq.VersionKey,
                        rolledBackBegin, long.MaxValue);

                    // Add the updated tail to the commit set
                    this.AddVersionToCommitSet(retryReq.TableId, retryReq.RecordKey, retryReq.VersionKey,
                        rolledBackBegin, TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP);
                }
                //else
                //{
                //    throw new TransactionException("An illegal state of the uploading phase.");
                //}
            }

			// Move on to the next phase
			this.writeKeyList = null;
            this.CurrentProc = this.setCommitTsProc;
			if (!this.DEBUG_MODE)
			{
				this.CurrentProc();
			}
            return;
        }

        private void AddVersionToAbortSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            if (!this.abortSet.ContainsKey(tableId))
            {
                this.abortSet[tableId] = new Dictionary<object, List<PostProcessingEntry>>();
            }

            if (!this.abortSet[tableId].ContainsKey(recordKey))
            {
                this.abortSet[tableId][recordKey] = new List<PostProcessingEntry>(TransactionExecution.POSTPROCESSING_LIST_MAX_CAPACITY);
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
                this.commitSet[tableId][recordKey] = new List<PostProcessingEntry>(TransactionExecution.POSTPROCESSING_LIST_MAX_CAPACITY);
            }
            this.commitSet[tableId][recordKey].Add(new PostProcessingEntry(versionKey, beginTs, endTs));
        }

        internal void SetCommitTimestamp()
        {
            if (this.commitTs < 0)
            {
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
            }

            // Haven't sent the SetCommitTime request
            if (this.requestStack.Count == 0)
            {
                SetCommitTsRequest setTsReq = this.executor.ResourceManager.SetCommitTsRequest(this.txId, this.commitTs);
                this.versionDb.EnqueueTxEntryRequest(this.txId, setTsReq);
                this.txReqGarbageQueue.Enqueue(setTsReq);

                this.requestStack.Push(setTsReq);
                return;
            }
            else if (this.requestStack.Count == 1)
            {
                if (!this.requestStack.Peek().Finished)
                {
                    // The prior request hasn't been processed. Returns the control to the caller.
                    return;
                }

                SetCommitTsRequest setTsReq = this.requestStack.Pop() as SetCommitTsRequest;
                long commitTime = setTsReq.Result == null ? -1 : (long)setTsReq.Result;
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
            //else
            //{
            //    throw new TransactionException("An illegal state when setting the tx's commit timestamp.");
            //}
        }

        internal void Validate()
        {
            // Have sent the GetVersionEntry request, but not received the response yet
            if (this.validateKeyList == null)
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

                    // Enqueues all requests re-reading the versions. 
                    // Enqueue requests if the request stack is not empty, if the current transaction 
                    // has no versions need to be validated, there is no need to return the control
                    if (this.requestStack.Count != 0)
                    {
                        return;
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
                this.validateKeyList = this.executor.ResourceManager.GetValidationKeyList();
                this.readVersionList = this.executor.ResourceManager.GetVersionList();
                foreach (TxRequest req in this.requestStack)
                {
                    ReadVersionRequest readVersionReq = req as ReadVersionRequest;
                    VersionEntry readEntry = req.Result as VersionEntry;
                    if (readEntry == null)
                    {
                        this.SetAbortMsg("read entry null");

                        this.executor.ResourceManager.RecycleVersionList(ref this.readVersionList);
                        this.executor.ResourceManager.RecycleValidationKeyList(ref this.validateKeyList);
                        // A really serious bug, should clear the stack before enter the next step
                        this.requestStack.Clear();
                        this.CurrentProc = this.abortProc;

						if (!this.DEBUG_MODE)
						{
							this.CurrentProc();
						}
                        return;
                    }

                    this.validateKeyList.Add(readVersionReq.TableId);
                    this.readVersionList.Add(readEntry);
                }

                this.requestStack.Clear();
            }

            // Already re-read the version entries back, need to check visiable
            while (this.validateKeyList.Count > 0 || this.requestStack.Count > 0)
            {
                int lastIndex = this.validateKeyList.Count - 1;
                // No concurrent txs hold the version or already received the response
                if (this.requestStack.Count == 0)
                {
                    // validateKeyList and readVersionList both have the same size, they share a lastIndex variable
                    string tableId = this.validateKeyList[lastIndex];
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
                            return;
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
                                this.validateKeyList.RemoveAt(lastIndex);
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
                        return;
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
                        this.validateKeyList.RemoveAt(lastIndex);
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
                            this.validateKeyList.RemoveAt(lastIndex);
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
                        return;
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
                        this.validateKeyList.RemoveAt(lastIndex);
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
                        this.validateKeyList.RemoveAt(lastIndex);
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
            this.validateKeyList = null;
            this.CurrentProc = new Procedure(this.WriteToLog);
			if (!this.DEBUG_MODE)
			{
				this.CurrentProc();
			}
            return;
        }

        internal void WriteToLog()
        {
            if (this.requestStack.Count == 0)
            {
                UpdateTxStatusRequest updateTxReq = this.executor.ResourceManager.UpdateTxStatusRequest(this.txId, TxStatus.Committed);
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

                this.isCommitted = true;
                this.requestStack.Pop();
                this.CurrentProc = this.commitPostproProc;
				if (!this.DEBUG_MODE)
				{
					this.CurrentProc();
				}
                return;
            }
        }

        internal void PostProcessingAfterCommit()
        {
            if (this.requestStack.Count == 0)
            {
                foreach (string tableId in this.commitSet.Keys)
                {
                    foreach (object recordKey in this.commitSet[tableId].Keys)
                    {
                        foreach (PostProcessingEntry entry in this.commitSet[tableId][recordKey])
                        {
                            if (entry.BeginTimestamp == TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP)
                            {
                                ReplaceVersionRequest replaceVerReq = this.executor.ResourceManager.ReplaceVersionRequest(
                                    tableId,
                                    recordKey,
                                    entry.VersionKey,
                                    this.commitTs,
                                    entry.EndTimestamp,
                                    VersionEntry.EMPTY_TXID,
                                    this.txId,
                                    -1);
                                this.versionDb.EnqueueVersionEntryRequest(tableId, replaceVerReq);
                                this.txReqGarbageQueue.Enqueue(replaceVerReq);

								this.requestStack.Push(replaceVerReq);
							}
                            else
                            {
                                // cloud environment: just replace the begin, end, txId field, need lua script, 3 redis command.
                                //ReplaceVersionRequest replaceVerReq = this.executor.ResourceManager.ReplaceVersionRequest(
                                //    tableId,
                                //    recordKey,
                                //    entry.VersionKey,
                                //    entry.BeginTimestamp,
                                //    this.commitTs,
                                //    VersionEntry.EMPTY_TXID,
                                //    this.txId,
                                //    long.MaxValue);
                                //this.versionDb.EnqueueVersionEntryRequest(tableId, replaceVerReq);
                                //this.txReqGarbageQueue.Enqueue(replaceVerReq);
                                //this.requestStack.Push(replaceVerReq);

                                // Single machine setting: pass the whole version, need only 1 redis command.
                                ReadSetEntry readEntry = this.readSet[tableId][recordKey];
								ReplaceWholeVersionRequest replaceWholeVerReq = this.executor.ResourceManager.ReplaceWholeVersionRequest(
									tableId,
									recordKey,
									entry.VersionKey,
									new VersionEntry(
										recordKey,
										entry.VersionKey,
										readEntry.BeginTimestamp,
										this.commitTs,
										readEntry.Record,
										VersionEntry.EMPTY_TXID,
										this.commitTs));

                                this.versionDb.EnqueueVersionEntryRequest(tableId, replaceWholeVerReq);
                                this.txReqGarbageQueue.Enqueue(replaceWholeVerReq);
								this.requestStack.Push(replaceWholeVerReq);
							}
                        }
                    }
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

                // All post-processing records have been uploaded.
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

        internal void PostProcessingAfterAbort()
        {
            if (this.requestStack.Count == 0)
            {
                foreach (string tableId in this.abortSet.Keys)
                {
                    foreach (object recordKey in this.abortSet[tableId].Keys)
                    {
                        foreach (PostProcessingEntry entry in this.abortSet[tableId][recordKey])
                        {
                            if (entry.BeginTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP)
                            {
                                DeleteVersionRequest delVerReq = this.executor.ResourceManager.DeleteVersionRequest(
                                    tableId, recordKey, entry.VersionKey);
                                this.versionDb.EnqueueVersionEntryRequest(tableId, delVerReq);
                                this.txReqGarbageQueue.Enqueue(delVerReq);
                                this.requestStack.Push(delVerReq);
                            }
                            else
                            {
                                ReplaceVersionRequest replaceVerReq = this.executor.ResourceManager.ReplaceVersionRequest(
                                    tableId,
                                    recordKey,
                                    entry.VersionKey,
                                    entry.BeginTimestamp,
                                    entry.EndTimestamp,
                                    VersionEntry.EMPTY_TXID,
                                    this.txId,
                                    long.MaxValue);
                                this.versionDb.EnqueueVersionEntryRequest(tableId, replaceVerReq);
                                this.txReqGarbageQueue.Enqueue(replaceVerReq);
                                this.requestStack.Push(replaceVerReq);
                            }
                        }
                    }
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
            if (this.readVersionList == null)
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
                    
                    return;
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
                        return;
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
                        this.readVersionList.RemoveAt(this.readVersionList.Count - 1);
                        continue;
                    }

                    // The last version entry is the one need to check whether visiable
                    VersionEntry versionEntry = this.readVersionList[this.readVersionList.Count - 1];
                    this.readVersionList.RemoveAt(this.readVersionList.Count - 1);

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
                        return;
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
                            this.readVersionList.RemoveAt(this.readVersionList.Count - 1);
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
            this.readVersionList = null;
            this.Progress = TxProgress.Open;
            received = true;
        }

        public void Commit()
        {
            Debug.Assert(this.Progress == TxProgress.Open);

            this.CurrentProc = this.uploadProc;
            this.CurrentProc();
            return;
        }
    }
}
