
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
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
        // Constant variables part
        public bool DEBUG_MODE = false;

        internal static bool TEST = false;

        internal static readonly long DEFAULT_TX_BEGIN_TIMESTAMP = -1L;

        internal static readonly int POSTPROCESSING_LIST_MAX_CAPACITY = 2;

        internal static readonly long UNSET_TX_COMMIT_TIMESTAMP = -2L;

        internal static readonly int TX_REQUEST_GARBAGE_QUEUE_SIZE = 30;


        // Private properties part
        private readonly ILogStore logStore;
        private readonly VersionDb versionDb;
        internal long txId;
        internal long commitTs;
        internal long maxCommitTsOfWrites;
        internal long beginTimestamp;
        internal TxStatus TxStatus;

        private readonly TxRange txRange;
        private readonly TransactionExecutor executor;
        private long beginTicks;
        internal Procedure CurrentProc { get; private set; }
        internal TxProgress Progress { get; private set; }
        internal StoredProcedure Procedure { get; set; }

        //Garbage Queue Part

        /// <summary>
        /// A garbage queue for tx requests
        /// all tx requests will be enqueued in the current execution and will be 
        /// recycled at the end of postprocessing phase
        /// </summary>
        internal readonly Queue<long> garbageQueueTxId;
        private readonly Queue<long> garbageQueueFinishTime;
        private readonly Queue<TxSetEntry> txSetEntryGCQueue;

        // entrySet part
        internal TxList<ReadSetEntry> readSet;
        internal TxList<WriteSetEntry> writeSet;
        internal TxList<PostProcessingEntry> abortSet;
        internal TxList<PostProcessingEntry> commitSet;
        internal TxList<VersionKeyEntry> largestVersionKeySet;

        // procedure part
        private readonly Procedure newTxIdProc;
        private readonly Procedure insertTxIdProc;
        private readonly Procedure recycleTxProc;
        private readonly Procedure commitTsProc;
        private readonly Procedure validateProc;
        private readonly Procedure abortPostproProc;
        private readonly Procedure uploadProc;
        private readonly Procedure updateTxProc;
        private readonly Procedure abortProc;
        private readonly Procedure commitVersionProc;
        private readonly Procedure readVersionListProc;
        private readonly Procedure readCheckVersionEntryProc;

        // request part
        private UploadVersionRequest uploadReq;
        private ReplaceVersionRequest replaceReq;
        private ReplaceVersionRequest retryReplaceReq;
        private GetTxEntryRequest getTxReq;
        private NewTxIdRequest newTxIdReq;
        private InsertTxIdRequest inserTxIdReq;
        private RecycleTxRequest recycleTxReq;
        private SetCommitTsRequest commitTsReq;
        private UpdateTxStatusRequest updateTxReq;
        private GetVersionListRequest getVListReq;
        private InitiGetVersionListRequest initiGetVListReq;
        private DeleteVersionRequest deleteReq;
        private ReadVersionRequest readReq;
        private UpdateVersionMaxCommitTsRequest updateMaxTsReq;
        private UpdateCommitLowerBoundRequest updateBoundReq;

        // List Resources
        private readonly TxList<VersionEntry> versionList;

        // Private variables to store temp values
        private string readTableId;
        private object readRecordKey;
        private int readEntryCount;
        private long readLargestVersionKey = -1;
        private int writeSetIndex = 0;

        // local version entry and tx entry instance
        private VersionEntry localVerEntry = new VersionEntry();
        private TxTableEntry localTxEntry = new TxTableEntry();

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
            this.txRange = txRange;
            this.executor = executor;
            this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.maxCommitTsOfWrites = -1L;
            this.beginTimestamp = TransactionExecution.DEFAULT_TX_BEGIN_TIMESTAMP;
            this.Progress = TxProgress.Open;
            this.Procedure = procedure;
            this.beginTicks = DateTime.Now.Ticks;

            this.readSet = new TxList<ReadSetEntry>();
            this.writeSet = new TxList<WriteSetEntry>();
            this.abortSet = new TxList<PostProcessingEntry>();
            this.commitSet = new TxList<PostProcessingEntry>();
            this.largestVersionKeySet = new TxList<VersionKeyEntry>();
            this.versionList = new TxList<VersionEntry>();

            // add 2 version entries to the list
            this.versionList.Add(new VersionEntry());
            this.versionList.Add(new VersionEntry());

            this.garbageQueueTxId = garbageQueueTxId;
            this.garbageQueueFinishTime = garbageQueueFinishTime;
            this.txSetEntryGCQueue = new Queue<TxSetEntry>(TX_REQUEST_GARBAGE_QUEUE_SIZE);

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
            this.readVersionListProc = new Procedure(this.ReadVersionList);
            this.readCheckVersionEntryProc = new Procedure(this.ReadCheckVersionEntry);

            this.readReq = new ReadVersionRequest(null, null, -1);
            this.uploadReq = new UploadVersionRequest(null, null, -1, null);
            this.replaceReq = new ReplaceVersionRequest(null, null, -1, -1, -1, -1, -1, -1);
            this.retryReplaceReq = new ReplaceVersionRequest(null, null, -1, -1, -1, -1, -1, -1);
            this.getTxReq = new GetTxEntryRequest(-1);
            this.newTxIdReq = new NewTxIdRequest(-1);
            this.inserTxIdReq = new InsertTxIdRequest(-1);
            this.recycleTxReq = new RecycleTxRequest(-1);
            this.commitTsReq = new SetCommitTsRequest(-1, -1);
            this.updateTxReq = new UpdateTxStatusRequest(-1, TxStatus.Ongoing);
            this.getVListReq = new GetVersionListRequest(null, null, null);
            this.initiGetVListReq = new InitiGetVersionListRequest(null, null);
            this.deleteReq = new DeleteVersionRequest(null, null, -1);
            this.updateBoundReq = new UpdateCommitLowerBoundRequest(-1, -1);
            this.updateMaxTsReq = new UpdateVersionMaxCommitTsRequest(null, null, -1, -1);
        }

        internal void Reset()
        {
            this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.maxCommitTsOfWrites = -1L;
            this.beginTimestamp = TransactionExecution.DEFAULT_TX_BEGIN_TIMESTAMP;
            this.TxStatus = TxStatus.Ongoing;

            this.Progress = TxProgress.Open;
            this.CurrentProc = null;
            this.beginTicks = DateTime.Now.Ticks;

            this.uploadReq.Free();
            this.replaceReq.Free();
            this.retryReplaceReq.Free();
            this.getTxReq.Free();
            this.newTxIdReq.Free();
            this.inserTxIdReq.Free();
            this.recycleTxReq.Free();
            this.commitTsReq.Free();
            this.updateTxReq.Free();
            this.getVListReq.Free();
            this.initiGetVListReq.Free();
            this.deleteReq.Free();
            this.updateBoundReq.Free();
            this.updateMaxTsReq.Free();

            this.readSet.Clear();
            this.writeSet.Clear();
            this.abortSet.Clear();
            this.commitSet.Clear();
            this.largestVersionKeySet.Clear();

            this.ClearLocalList();
            this.writeSetIndex = 0;
            this.readLargestVersionKey = -1;
            // init and get tx id
            this.InitTx();
            //this.txId = this.txRange.RangeStart;
        }

        internal void SetAbortMsg(string msg)
        {
            //TxAbortReasonTracer.reasons[this.Procedure.pid] = msg;
        }

        internal void InitTx()
        {
            this.Progress = TxProgress.Initi;

            if (this.garbageQueueTxId != null && this.garbageQueueTxId.Count > 0)
            {
                long candidate = this.garbageQueueTxId.Peek();
                long finishTime = this.garbageQueueFinishTime.Peek();

                if (DateTime.Now.Ticks - finishTime >= TransactionExecutor.elapsed)
                {
                    this.garbageQueueTxId.Dequeue();
                    this.garbageQueueFinishTime.Dequeue();

                    this.recycleTxReq.Set(candidate);
                    this.recycleTxReq.Use();
                    this.versionDb.EnqueueTxEntryRequest(candidate, this.recycleTxReq, this.executor.Partition);

                    this.CurrentProc = this.recycleTxProc;
                    this.RecycleTx();
                    return;
                }
            }

            long id = this.txRange.NextTxCandidate();
            this.newTxIdReq.Set(id);
            this.newTxIdReq.Use();
            this.versionDb.EnqueueTxEntryRequest(id, this.newTxIdReq, this.executor.Partition);
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
                this.newTxIdReq.Set(newId);
                this.newTxIdReq.Use();
                this.versionDb.EnqueueTxEntryRequest(newId, this.newTxIdReq, this.executor.Partition);

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
            this.newTxIdReq.Free();

            this.inserTxIdReq.Set(txId);
            this.inserTxIdReq.Use();
            this.versionDb.EnqueueTxEntryRequest(this.txId, this.inserTxIdReq, this.executor.Partition);

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
            this.inserTxIdReq.Free();
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
            this.recycleTxReq.Free();
            this.CurrentProc = null;
            this.Progress = TxProgress.Open;
        }

        internal void Upload()
        {
            this.Progress = TxProgress.Final;
            // Here we couldn't pop all writeSetEntry, since we need to determine whether
            // valdiating a key in readSet by writeSetEntry
            while (this.writeSetIndex < this.writeSet.Count ||
                    this.replaceReq.IsActive() ||
                    this.uploadReq.IsActive() ||
                    this.getTxReq.IsActive() ||
                    this.retryReplaceReq.IsActive())
            {
                // Prior write set entry has been uploaded successfully
                if (!this.replaceReq.IsActive() &&
                    !this.uploadReq.IsActive() &&
                    !this.getTxReq.IsActive() &&
                    !this.retryReplaceReq.IsActive())
                {
                    WriteSetEntry writeEntry = this.writeSet[this.writeSetIndex++];
                    string tableId = writeEntry.TableId;
                    object recordKey = writeEntry.RecordKey;

                    object payload = writeEntry.Payload;
                    // should check the type of writes, insert/update/delete
                    // The write-set record is an insert or update record, try to insert the new version
                    if (payload != null)
                    {
                        VersionEntry newImageEntry = new VersionEntry(
                                writeEntry.RecordKey,
                                writeEntry.VersionKey,
                                payload,
                                this.txId);

                        this.uploadReq.Set(tableId, recordKey, newImageEntry.VersionKey, newImageEntry);
                        this.uploadReq.Use();
                        this.versionDb.EnqueueVersionEntryRequest(tableId, this.uploadReq, this.executor.Partition);
                    }
                    // The write-set record is an delete record, only replace the old version
                    else
                    {
                        ReadSetEntry readVersion = this.FindReadSetEntry(tableId, recordKey);
                        this.replaceReq.Set(tableId, recordKey, readVersion.VersionKey, readVersion.BeginTimestamp, long.MaxValue,
                            this.txId, VersionEntry.EMPTY_TXID, long.MaxValue, this.localVerEntry);
                        this.replaceReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(tableId, this.replaceReq, this.executor.Partition);
                    }
                }
                else if (this.uploadReq.IsActive() && !this.replaceReq.IsActive() &&
                    !this.retryReplaceReq.IsActive() && !this.getTxReq.IsActive())
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

                    this.uploadReq.Free();

                    ReadSetEntry readVersion = this.FindReadSetEntry(tableId, recordKey);
                    if (readVersion == null)
                    {
                        // The write-set record is an insert record. 
                        // Moves to the next write-set record or the next phase.
                        continue;
                    }

                    // Initiates a new request to append the new image to the tail of the version list.
                    // The tail entry could be [Ts, inf, -1], [Ts, inf, txId1] or [-1, -1, txId1].
                    // The first case indicates that no concurrent tx is locking the tail.
                    // The second case indicates that one concurrent tx is holding the tail. 
                    // The third case means that a concurrent tx is creating a new tail, which was seen by this tx. 
                    this.replaceReq.Set(tableId, recordKey, readVersion.VersionKey, readVersion.BeginTimestamp,
                        long.MaxValue, this.txId, VersionEntry.EMPTY_TXID, long.MaxValue, this.localVerEntry);
                    this.replaceReq.Use();

                    this.versionDb.EnqueueVersionEntryRequest(tableId, this.replaceReq, this.executor.Partition);
                }
                else if (this.replaceReq.IsActive() && !this.uploadReq.IsActive() &&
                    !this.retryReplaceReq.IsActive() && !this.getTxReq.IsActive())
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

                        this.replaceReq.Free();
                        // Move on to the next write-set record or the next phase
                        continue;
                    }
                    else if (versionEntry.TxId >= 0)
                    {
                        // The first try was unsuccessful because the tail is hold by another concurrent tx. 
                        // If the concurrent tx has finished (committed or aborted), there is a chance for this tx
                        // to re-gain the lock. 
                        // Enqueues a request to check the status of the tx that is holding the tail.
                        this.getTxReq.Set(versionEntry.TxId);
                        this.getTxReq.Use();
                        this.versionDb.EnqueueTxEntryRequest(versionEntry.TxId, this.getTxReq, this.executor.Partition);

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
                else if (this.replaceReq.IsActive() && this.getTxReq.IsActive() &&
                    !this.retryReplaceReq.IsActive() && !this.uploadReq.IsActive())
                {
                    if (!this.getTxReq.Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    TxTableEntry conflictTxStatus = this.getTxReq.Result as TxTableEntry;
                    this.getTxReq.Free();

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

                        this.replaceReq.Set(replaceReq.TableId, replaceReq.RecordKey, versionEntry.VersionKey, conflictTxStatus.CommitTime,
                            long.MaxValue, this.txId, conflictTxStatus.TxId, VersionEntry.DEFAULT_END_TIMESTAMP, this.localVerEntry);
                        this.retryReplaceReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(replaceReq.TableId, this.retryReplaceReq, this.executor.Partition);
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
                            this.replaceReq.Set(replaceReq.TableId, replaceReq.RecordKey, versionEntry.VersionKey, versionEntry.BeginTimestamp,
                                long.MaxValue, this.txId, conflictTxStatus.TxId, long.MaxValue, this.localVerEntry);
                            this.retryReplaceReq.Use();

                            this.versionDb.EnqueueVersionEntryRequest(replaceReq.TableId, this.retryReplaceReq, this.executor.Partition);
                        }
                    }
                }
                else if (this.replaceReq.IsActive() && this.retryReplaceReq.IsActive() &&
                    !this.uploadReq.IsActive() && !this.getTxReq.IsActive())
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

                    this.replaceReq.Free();
                    this.retryReplaceReq.Free();
                }
                else
                {
                    throw new TransactionException("An illegal state of the uploading phase.");
                }
            }

            this.SetCommitTimestamp();
        }

        private void AddVersionToAbortSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            PostProcessingEntry abortEntry = this.executor.ResourceManager.GetPostProcessingEntry(
                tableId, recordKey, versionKey, beginTs, endTs);
            this.txSetEntryGCQueue.Enqueue(abortEntry);
            this.abortSet.Add(abortEntry);
        }

        private void AddVersionToCommitSet(string tableId, object recordKey, long versionKey, long beginTs, long endTs)
        {
            PostProcessingEntry commitEntry = this.executor.ResourceManager.GetPostProcessingEntry(
                tableId, recordKey, versionKey, beginTs, endTs);
            this.txSetEntryGCQueue.Enqueue(commitEntry);
            this.commitSet.Add(commitEntry);
        }

        internal void SetCommitTimestamp()
        {
            Debug.Assert(this.commitTs < 0);

            long proposedCommitTs = this.maxCommitTsOfWrites + 1;
            int size = this.readSet.Count;
            for (int i = 0; i < size; i++)
            {
                ReadSetEntry entry = this.readSet[i];
                if (this.FindWriteSetEntry(entry.TableId, entry.RecordKey) != null)
                {
                    proposedCommitTs = Math.Max(proposedCommitTs, entry.BeginTimestamp + 1);
                }
                else
                {
                    proposedCommitTs = Math.Max(proposedCommitTs, entry.BeginTimestamp);
                }
            }

            this.commitTs = proposedCommitTs;

            this.commitTsReq.Set(this.txId, this.commitTs);
            this.commitTsReq.Use();
            this.versionDb.EnqueueTxEntryRequest(this.txId, this.commitTsReq, this.executor.Partition);

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
            this.commitTsReq.Free();
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
            while (!this.readSet.IsEmpty || this.readReq.IsActive() ||
                this.getTxReq.IsActive() || this.updateMaxTsReq.IsActive() ||
                this.updateBoundReq.IsActive())
            {
                // validate new version
                if (!this.readReq.IsActive() && !this.getTxReq.IsActive() &&
                    !this.updateMaxTsReq.IsActive() && !this.updateBoundReq.IsActive())
                {
                    ReadSetEntry entry = this.readSet.PopRight();
                    if (this.FindWriteSetEntry(entry.TableId, entry.Record) != null)
                    {
                        continue;
                    }

                    this.readReq.Set(entry.TableId, entry.RecordKey, entry.VersionKey, this.localVerEntry);
                    this.readReq.Use();

                    this.versionDb.EnqueueVersionEntryRequest(entry.TableId, readReq, this.executor.Partition);
                }
                else if (this.readReq.IsActive() && !this.getTxReq.IsActive() &&
                    !this.updateMaxTsReq.IsActive() && !this.updateBoundReq.IsActive())
                {
                    if (!this.readReq.Finished)
                    {
                        return;
                    }

                    VersionEntry readVersion = this.readReq.Result as VersionEntry;
                    string tableId = this.readReq.TableId;
                    this.readReq.Free();

                    if (readVersion == null)
                    {
                        this.SetAbortMsg("read entry null");
                        this.CurrentProc = this.abortProc;

                        if (!this.DEBUG_MODE)
                        {
                            this.CurrentProc();
                        }
                        return;
                    }

                    if (readVersion.MaxCommitTs >= this.commitTs)
                    {
                        // No need to update the version's maxCommitTs.
                        // Check whether or not the re-read version is occupied by another tx.
                        if (readVersion.TxId != VersionEntry.EMPTY_TXID)
                        {
                            // A concurrent tx is locking the version. Checks the tx's status to decide how to move forward, 
                            // i.e., abort or pass validation.
                            this.getTxReq.Set(readVersion.TxId);
                            this.getTxReq.Use();
                            this.versionDb.EnqueueTxEntryRequest(readVersion.TxId, getTxReq, this.executor.Partition);
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
                                continue;
                            }
                        }
                    }
                    else
                    {
                        // Updates the version's max commit timestamp
                        this.updateMaxTsReq.Set(tableId, readVersion.RecordKey, readVersion.VersionKey, this.commitTs, this.localVerEntry);
                        this.updateMaxTsReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(tableId, updateMaxTsReq, this.executor.Partition);
                    }
                }
                else if (this.updateMaxTsReq.IsActive() && !this.getTxReq.IsActive() &&
                    !this.readReq.IsActive() && !this.updateBoundReq.IsActive())
                {
                    if (!this.updateMaxTsReq.Finished)
                    {
                        return;
                    }

                    VersionEntry readEntry = updateMaxTsReq.Result as VersionEntry;
                    this.updateMaxTsReq.Free();
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
                }
                else if (this.getTxReq.IsActive() && !this.readReq.IsActive() &&
                    !this.updateMaxTsReq.IsActive() && !this.updateBoundReq.IsActive())
                {
                    if (!this.getTxReq.Finished)
                    {
                        return;
                    }

                    TxTableEntry txEntry = this.getTxReq.Result as TxTableEntry;
                    this.getTxReq.Free();

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
                            continue;
                        }
                    }
                    else
                    {
                        this.updateBoundReq.Set(txEntry.TxId, this.commitTs+1);
                        this.updateBoundReq.Use();

                        this.versionDb.EnqueueTxEntryRequest(txEntry.TxId, this.updateBoundReq, this.executor.Partition);
                    }
                }
                else if (this.updateBoundReq.IsActive() && !this.readReq.IsActive() &&
                    !this.getTxReq.IsActive() && !this.updateMaxTsReq.IsActive())
                {
                    if (!this.updateBoundReq.Finished)
                    {
                        return;
                    }

                    long txCommitTs = this.updateBoundReq.Result == null ?
                        VersionDb.RETURN_ERROR_CODE : (long)this.updateBoundReq.Result;
                    this.updateBoundReq.Free();

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
                        continue;
                    }
                }
            }
            this.WriteToLog();
        }

        internal void WriteToLog()
        {
            this.updateTxReq.TxId = this.txId;
            this.updateTxReq.TxStatus = TxStatus.Committed;
            this.updateTxReq.Use();

            this.versionDb.EnqueueTxEntryRequest(this.txId, this.updateTxReq, this.executor.Partition);
            this.CurrentProc = this.updateTxProc;
            this.UpdateTxStatus();
        }

        internal void UpdateTxStatus()
        {
            if (!this.updateTxReq.Finished)
            {
                return;
            }

            this.updateTxReq.Free();
            this.TxStatus = TxStatus.Committed;
            this.PostProcessingAfterCommit();
        }

        internal void PostProcessingAfterCommit()
        {
            while (!this.commitSet.IsEmpty || this.replaceReq.IsActive())
            {
                if (!this.replaceReq.IsActive())
                {
                    PostProcessingEntry entry = this.commitSet.PopRight();
                    if (entry.BeginTimestamp == TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP)
                    {
                        this.replaceReq.Set(entry.TableId, entry.RecordKey, entry.VersionKey, this.commitTs, entry.EndTimestamp, 
                            VersionEntry.EMPTY_TXID, this.txId, VersionEntry.DEFAULT_END_TIMESTAMP, this.localVerEntry);

                        this.replaceReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.replaceReq, this.executor.Partition);
                    }
                    else
                    {
                        this.replaceReq.Set(entry.TableId, entry.RecordKey, entry.VersionKey, entry.BeginTimestamp, this.commitTs,
                            VersionEntry.EMPTY_TXID, this.txId, long.MaxValue, this.localVerEntry);
                        this.replaceReq.Use();
                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.replaceReq, this.executor.Partition);

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
                }
                else if (this.replaceReq != null)
                {
                    if (!this.replaceReq.Finished)
                    {
                        return;
                    }
                    this.replaceReq.Free();
                }
            }

            this.CurrentProc = this.commitVersionProc;
            this.CommitModifications();
        }

        internal void CommitModifications()
        {
            while (this.txSetEntryGCQueue.Count > 0)
            {
                TxSetEntry entry = this.txSetEntryGCQueue.Dequeue();
                this.executor.ResourceManager.RecycleTxSetEntry(ref entry);
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

        internal void PostProcessingAfterAbort()
        {
            while (!this.abortSet.IsEmpty || this.replaceReq.IsActive() || this.deleteReq.IsActive())
            {
                if (!this.replaceReq.IsActive() && !this.deleteReq.IsActive())
                {
                    PostProcessingEntry entry = this.abortSet.PopRight();
                    if (entry.BeginTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP)
                    {
                        this.deleteReq.Set(entry.TableId, entry.RecordKey, entry.VersionKey);
                        this.deleteReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.deleteReq, this.executor.Partition);
                    }
                    else
                    {
                        this.replaceReq.Set(entry.TableId, entry.RecordKey, entry.VersionKey, entry.BeginTimestamp,
                            entry.EndTimestamp, VersionEntry.EMPTY_TXID, this.txId, long.MaxValue, this.localVerEntry);

                        this.replaceReq.Use();
                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.replaceReq, this.executor.Partition);
                    }
                }
                else if (this.replaceReq.IsActive())
                {
                    if (!this.replaceReq.Finished)
                    {
                        return;
                    }
                    this.replaceReq.Free();
                }
                else if (this.deleteReq.IsActive())
                {
                    if (!this.deleteReq.Finished)
                    {
                        return;
                    }
                    this.deleteReq.Free();
                }
            }

            while (this.txSetEntryGCQueue.Count > 0)
            {
                TxSetEntry entry = this.txSetEntryGCQueue.Dequeue();
                this.executor.ResourceManager.RecycleTxSetEntry(ref entry);
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

        /// <summary>
        /// Abort if timeout
        /// </summary>
        internal void TimeoutAbort()
        {
            this.SetAbortMsg("time out abort");
            this.CurrentProc = this.abortProc;
        }

        internal void Abort()
        {
            this.Progress = TxProgress.Final;
            this.replaceReq = null;

            if (!this.updateTxReq.IsActive())
            {
                this.updateTxReq.Set(this.txId, TxStatus.Aborted);
                this.updateTxReq.Use();

                this.versionDb.EnqueueTxEntryRequest(this.txId, updateTxReq, this.executor.Partition);
                return;
            }
            else if (this.updateTxReq.IsActive())
            {
                if (!this.updateTxReq.Finished)
                {
                    return;
                }
                this.updateTxReq.Free();

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
            WriteSetEntry writeEntry = this.FindWriteSetEntry(tableId, recordKey);
            ReadSetEntry readEntry = this.FindReadSetEntry(tableId, recordKey);
            if (writeEntry != null)
            {
                if (writeEntry.Payload != null)
                {
                    this.SetAbortMsg("write set tableid recordkey null");
                    this.CurrentProc = this.abortProc;
                    this.CurrentProc();
                    //throw new TransactionException("Cannot insert the same record key twice.");
                }
                else
                {
                    writeEntry.Payload = record;
                }
            }
            // Checks whether the record is already in the local read set
            else if (readEntry != null)
            {
                this.SetAbortMsg("record is already in the local read set");
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
                //throw new TransactionException("The same record already exists.");
            }
            // Neither the readSet and writeSet have the recordKey
            else
            {
                VersionKeyEntry versionKeyEntry = this.FindVersionKeyEntry(tableId, recordKey);
                WriteSetEntry entry = this.executor.ResourceManager.GetWriteSetEntry(
                    tableId, recordKey, record, versionKeyEntry.VersionKey + 1);
                this.writeSet.Add(entry);
                this.txSetEntryGCQueue.Enqueue(entry);
            }
            this.Procedure?.InsertCallBack(tableId, recordKey, record);
        }

        public void Read(string tableId, object recordKey, out bool received, out object payload)
        { 
            this.Read(tableId, recordKey, false, out received, out payload);
        }

        public void Update(string tableId, object recordKey, object payload)
        {
            WriteSetEntry writeEntry = this.FindWriteSetEntry(tableId, recordKey);
            ReadSetEntry readEntry = this.FindReadSetEntry(tableId, recordKey);
            if (writeEntry != null)
            {
                if (writeEntry.Payload != null)
                {
                    writeEntry.Payload = payload;
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
            else if (readEntry != null)
            {
                WriteSetEntry entry = this.executor.ResourceManager.GetWriteSetEntry(
                    tableId, recordKey, payload, readEntry.VersionKey + 1);
                this.writeSet.Add(entry);
                this.txSetEntryGCQueue.Enqueue(entry);
            }
            else
            {
                this.SetAbortMsg("update fail, some reason");
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
                //throw new TransactionException("The record has not been read or does not exist. Cannot update it.");
            }

            this.Procedure?.UpdateCallBack(tableId, recordKey, payload);
        }

        public void Delete(string tableId, object recordKey, out object payload)
        {
            payload = null;
            WriteSetEntry writeEntry = this.FindWriteSetEntry(tableId, recordKey);
            ReadSetEntry readEntry = this.FindReadSetEntry(tableId, recordKey);
            if (writeEntry != null)
            {
                if (writeEntry != null)
                {
                    payload = writeEntry.Payload;
                    writeEntry.Payload = null;
                }
                else
                {
                    this.SetAbortMsg("delete fail reason1");
                    this.CurrentProc = this.abortProc;
                    this.CurrentProc();
                    // throw new TransactionException("The record to be deleted has been deleted by the same tx.");
                }
            }
            else if (readEntry != null)
            {
                payload = readEntry.Record;

                WriteSetEntry entry = this.executor.ResourceManager.GetWriteSetEntry(tableId, recordKey, null, -1);
                this.writeSet.Add(entry);
                this.txSetEntryGCQueue.Enqueue(entry);
            }
            else
            {
                this.SetAbortMsg("delete fail reason2");
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
                // throw new TransactionException("The record has not been read or does not exist. Cannot delete it.");
            }
            this.Procedure?.DeleteCallBack(tableId, recordKey, payload);
        }

        public void ReadAndInitialize(string tableId, object recordKey, out bool received, out object payload)
        {
            this.Read(tableId, recordKey, true, out received, out payload);
        }

        private void Read(string tableId, object recordKey, bool initi, out bool received, out object payload)
        {
            this.readLargestVersionKey = -1;
            this.readTableId = tableId;
            this.readRecordKey = recordKey;

            received = false;
            payload = null;

            if (this.writeSet.Count > 0)
            {
                WriteSetEntry writeEntry = this.FindWriteSetEntry(tableId, recordKey);
                if (writeEntry != null)
                {
                    received = true;
                    payload = writeEntry.Payload;
                    this.Procedure?.ReadCallback(tableId, recordKey, payload);
                    return;
                }
            }

            if (this.readSet.Count > 0)
            {
                ReadSetEntry readEntry = this.FindReadSetEntry(tableId, recordKey);
                if (readEntry != null)
                {
                    payload = readEntry.Record;
                    received = true;
                    this.Procedure?.ReadCallback(tableId, recordKey, payload);
                    return;
                }
            }

            // if the version entry would be read is not in the local version list,
            // the tx should send requests to get version list from storage, set the Progress as READ 
            // to prevent other operations.
            this.Progress = TxProgress.Read;

            if (initi)
            {
                this.initiGetVListReq.Set(tableId, recordKey, this.versionList);
                this.initiGetVListReq.Use();

                this.versionDb.EnqueueVersionEntryRequest(tableId, this.initiGetVListReq, this.executor.Partition);
            }
            else
            {
                this.getVListReq.Set(tableId, recordKey, this.versionList);
                this.getVListReq.Use();

                this.versionDb.EnqueueVersionEntryRequest(tableId, this.getVListReq, this.executor.Partition);
            }

            this.CurrentProc = this.readVersionListProc;
            this.CurrentProc();
        }

        internal void ReadVersionList()
        {
            // The reqeust have been sent and now waits for response from GetVersionList request
            if (this.getVListReq.IsActive())
            {
                if (!this.getVListReq.Finished)
                {
                    return;
                }

                this.readEntryCount = (int)this.getVListReq.Result;
                this.getVListReq.Free();
                
                // The local version list was assigned to the get-version-list request.
                // By the time the request returns, the list has been filled.
                if (this.readEntryCount == 0)
                {
                    // No versions for the record has been found.
                    this.Progress = TxProgress.Open;
                    this.CurrentProc = null;
                    this.Procedure?.ReadCallback(this.readTableId, this.readRecordKey, null);
                    return;
                }

                // Sort the version list by the descending order of version keys.
                this.versionList.Sort(this.readEntryCount);
                this.CurrentProc = this.readCheckVersionEntryProc;
                this.CurrentProc();
            }
            else if (this.initiGetVListReq.IsActive() && !this.getVListReq.IsActive())
            {
                if (!this.initiGetVListReq.Finished)
                {
                    return;
                }

                // The record did not exist. 
                // The request successfully initialized a version list for the record. 
                if ((long)this.initiGetVListReq.Result == 1)
                {
                    VersionKeyEntry versionKeyEntry = this.executor.ResourceManager.GetVersionKeyEntry(
                        this.readTableId, this.readRecordKey, VersionEntry.VERSION_KEY_STRAT_INDEX);
                    this.largestVersionKeySet.Add(versionKeyEntry);
                    this.txSetEntryGCQueue.Enqueue(versionKeyEntry);

                    this.initiGetVListReq.Free();
                    // No read call back is invoked. 
                    this.Progress = TxProgress.Open;
                    this.CurrentProc = null;
                    this.Procedure?.ReadCallback(this.readTableId, this.readRecordKey, null);
                    return;
                }
                else
                {
                    this.getVListReq.Set(this.readTableId, this.readRecordKey, this.versionList);
                    this.getVListReq.Use();

                    this.versionDb.EnqueueVersionEntryRequest(this.readTableId, this.getVListReq, this.executor.Partition);

                    this.CurrentProc = this.readVersionListProc;
                    this.CurrentProc();
                }
            }
        }

        internal void ReadCheckVersionEntry()
        {
            VersionEntry visibleVersion = null;
            // Keep a committed version to retrieve the largest version key
            VersionEntry committedVersion = null;
            while (this.readEntryCount > 0)
            {
                // Wait for the GetTxEntry response
                if (this.getTxReq.IsActive())
                {
                    if (!this.getTxReq.Finished)
                    {
                        return;
                    }

                    TxTableEntry pendingTx = this.getTxReq.Result as TxTableEntry;
                    this.getTxReq.Free();

                    if (pendingTx == null)
                    {
                        // Failed to retrieve the status of the tx holding the version. 
                        // Moves on to the next version.
                        this.readEntryCount--;
                        continue;
                    }

                    // The last version entry is the one need to check whether visiable
                    VersionEntry versionEntry = this.versionList[this.readEntryCount];
                    this.readEntryCount--;

                    // If the version entry is a dirty write, skips the entry.
                    if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP &&
                        (pendingTx.Status == TxStatus.Ongoing || pendingTx.Status == TxStatus.Aborted))
                    {
                        continue;
                    }

                    // The current version is commited and should be extracted the largest version key
                    committedVersion = versionEntry;

                    // The version list is traversed in the descending order of version keys.
                    // The first committed version sets readLargestVersionKey.
                    if (this.readLargestVersionKey < 0)
                    {
                        this.readLargestVersionKey = versionEntry.VersionKey;
                    }

                    // A dirty write has been appended after this version entry. 
                    // This version is visible if the writing tx has not been committed 
                    if (versionEntry.EndTimestamp == long.MaxValue && pendingTx.Status != TxStatus.Committed)
                    {
                        visibleVersion = versionEntry;
                    }
                    // A dirty write is visible to this tx when the writing tx has been committed, 
                    // which has not finished postprocessing and changing the dirty write to a normal version entry
                    else if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP &&
                        pendingTx.Status == TxStatus.Committed)
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
                else
                {
                    VersionEntry versionEntry = this.versionList[this.readEntryCount - 1];

                    if (versionEntry.TxId >= 0)
                    {
                        // Send the GetTxEntry request
                        this.getTxReq.Set(versionEntry.TxId);
                        this.getTxReq.Use();

                        this.versionDb.EnqueueTxEntryRequest(versionEntry.TxId, getTxReq, this.executor.Partition);
                    }
                    else
                    {
                        committedVersion = versionEntry;
                        // The version list is traversed in the descending order of version keys.
                        // The first committed version sets readLargestVersionKey.
                        if (this.readLargestVersionKey < 0)
                        {
                            this.readLargestVersionKey = versionEntry.VersionKey;
                        }

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
                            this.readEntryCount--;
                        }
                    }
                }

                // Break the loop once find a visiable version
                if (visibleVersion != null)
                {
                    break;
                }
            }

            object payload = null;
            // Put the visible version into the read set. 
            if (visibleVersion != null)
            {
                payload = visibleVersion.Record;

                // Add the record to local readSet
                ReadSetEntry readEntry = this.executor.ResourceManager.GetReadSetEntry(
                    this.readTableId,
                    this.readRecordKey,
                    visibleVersion.VersionKey,
                    visibleVersion.BeginTimestamp,
                    visibleVersion.EndTimestamp,
                    visibleVersion.TxId,
                    visibleVersion.Record,
                    this.readLargestVersionKey);

                this.readSet.Add(readEntry);
                this.txSetEntryGCQueue.Enqueue(readEntry);
            }

            this.Progress = TxProgress.Open;
            this.CurrentProc = null;
            // Read call back
            this.Procedure?.ReadCallback(this.readTableId, this.readRecordKey, payload);
        }

        public void Commit()
        {
            this.CurrentProc = this.uploadProc;
            this.CurrentProc();
            //this.WriteToLog();
        }

        private void ClearLocalList()
        {
            this.readSet.Clear();
            this.writeSet.Clear();
            this.commitSet.Clear();
            this.abortSet.Clear();
            this.largestVersionKeySet.Clear();
        }

        private ReadSetEntry dummyReadSetEntry = new ReadSetEntry();
        private ReadSetEntry FindReadSetEntry(string tableId, object recordKey)
        {
            dummyReadSetEntry.TableId = tableId;
            dummyReadSetEntry.RecordKey = recordKey;
            return this.readSet.Find(dummyReadSetEntry);
        }

        private WriteSetEntry dummyWriteSetEntry = new WriteSetEntry();
        private WriteSetEntry FindWriteSetEntry(string tableId, object recordKey)
        {
            dummyWriteSetEntry.TableId = tableId;
            dummyWriteSetEntry.RecordKey = recordKey;
            return this.writeSet.Find(dummyWriteSetEntry);
        }

        private VersionKeyEntry dummyVersionKeyEntry = new VersionKeyEntry();
        private VersionKeyEntry FindVersionKeyEntry(string tableId, object recordKey)
        {
            dummyVersionKeyEntry.TableId = tableId;
            dummyVersionKeyEntry.RecordKey = recordKey;
            return this.largestVersionKeySet.Find(dummyVersionKeyEntry);
        }
    }
}
