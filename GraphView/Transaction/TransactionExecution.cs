
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

        internal static readonly int DEFAULT_SET_SIZE = 8;

        // Private properties part
        private readonly ILogStore logStore;
        private readonly VersionDb versionDb;
        internal long txId;
        internal long commitTs;
        internal long maxCommitTsOfWrites;
        internal long beginTimestamp;
        internal TxStatus TxStatus;

        private readonly TxRange txRange;
        private readonly int execId;
        private long beginTicks;
        internal Procedure CurrentProc { get; private set; }
        internal TxProgress Progress { get; private set; }
        internal StoredProcedure Procedure { get; set; }

        private TxResourceManager resourceManager;

        // entrySet part
        internal TxObjPoolList<ReadSetEntry> readSet;
        internal TxObjPoolList<WriteSetEntry> writeSet;
        internal TxObjPoolList<PostProcessingEntry> abortSet;
        internal TxObjPoolList<PostProcessingEntry> commitSet;
        internal TxObjPoolList<VersionKeyEntry> largestVersionKeySet;

        // procedure part
        private readonly Procedure newTxIdProc;
        private readonly Procedure insertTxIdProc;
        private readonly Procedure recycleTxProc;
        private readonly Procedure commitTsProc;
        private readonly Procedure validateProc;
        private readonly Procedure abortPostproProc;
        private readonly Procedure uploadProc;
        private readonly Procedure updateTxCommitProc;
        private readonly Procedure abortProc;
        private readonly Procedure commitVersionProc;
        private readonly Procedure readVersionListProc;
        private readonly Procedure readCheckVersionEntryProc;
        private readonly Procedure updateTxAbortProc;
        private readonly Procedure commitPostproProc;

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
        // Those references of remote entries will be stored in the remoteVersionList to 
        // get the right pointer of remote version entry
        private readonly TxList<VersionEntry> remoteVersionRefList;
        private IDictionary<long, VersionEntry> remoteVerListRef;
        // Private variables to store temp values
        // record the tableId and recordKey between funcs in read operation
        private string readTableId;
        private object readRecordKey;
        // The number of entries in read operation
        private int readEntryCount;
        private long readLargestVersionKey = VersionEntry.VERSION_KEY_START_INDEX;
        internal object ReadPayload { get; private set; }

        // The writeSet index to tranverse the writeset
        private int writeSetIndex = 0;

        // The pointer to re-read version entry and replace version entry
        private VersionEntry rereadVerEntry = null;
        private VersionEntry replaceVerEntry = null;
        private VersionEntry replaceRemoteVerRef = null;
        private WriteSetEntry currentWriteSetEntry = null;
        private string replaceTableId;

        // local version entry (remote version entry is stored in readSet)
        private VersionEntry localVerEntry = new VersionEntry();

        private VersionEntry localUploadVerEntry = new VersionEntry();

        // local and remote txEntry of current tx
        private TxTableEntry localTxEntry = new TxTableEntry();
        private TxTableEntry remoteTxEntryRef;

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
            TxRange txRange = null,
            int execId = 0,
            TxResourceManager txResourceManager = null)
        {
            this.logStore = logStore;
            this.versionDb = versionDb;
            this.txRange = txRange;
            this.execId = execId;
            this.commitTs = TxTableEntry.DEFAULT_COMMIT_TIME;
            this.maxCommitTsOfWrites = -1L;
            this.beginTimestamp = TransactionExecution.DEFAULT_TX_BEGIN_TIMESTAMP;
            this.Progress = TxProgress.Open;
            this.Procedure = procedure;
            this.beginTicks = DateTime.Now.Ticks;

            this.readSet = new TxObjPoolList<ReadSetEntry>();
            this.writeSet = new TxObjPoolList<WriteSetEntry>();
            this.abortSet = new TxObjPoolList<PostProcessingEntry>();
            this.commitSet = new TxObjPoolList<PostProcessingEntry>();
            this.largestVersionKeySet = new TxObjPoolList<VersionKeyEntry>();

            this.versionList = new TxList<VersionEntry>();
            this.remoteVersionRefList = new TxList<VersionEntry>();

            // add 2 version entries to the list
            this.versionList.Add(new VersionEntry());
            this.versionList.Add(new VersionEntry());

            this.validateProc = new Procedure(this.Validate);
            this.uploadProc = new Procedure(this.Upload);
            this.abortPostproProc = new Procedure(this.PostProcessingAfterAbort);
            this.abortProc = new Procedure(this.Abort);
            this.newTxIdProc = new Procedure(this.NewTxId);
            this.newTxIdProc = new Procedure(this.NewTxId);
            this.recycleTxProc = new Procedure(this.RecycleTx);
            this.commitTsProc = new Procedure(this.FinalizeCommitTs);
            this.updateTxCommitProc = new Procedure(this.UpdateTxStatusToCommited);
            this.updateTxAbortProc = new Procedure(this.UpdateTxStatusToAborted);
            this.commitVersionProc = new Procedure(this.CommitModifications);
            this.readVersionListProc = new Procedure(this.ReadVersionList);
            this.readCheckVersionEntryProc = new Procedure(this.ReadCheckVersionEntry);
            this.commitPostproProc = new Procedure(this.PostProcessingAfterCommit);

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

            this.remoteTxEntryRef = null;

            this.resourceManager = txResourceManager;
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
            this.remoteVersionRefList.Clear();

            this.writeSetIndex = 0;
            this.readLargestVersionKey = VersionEntry.VERSION_KEY_START_INDEX;
            //this.txId = this.txRange.RangeStart;

            this.rereadVerEntry = null;
            this.replaceVerEntry = null;
            // TODO: could optimize it
            this.remoteTxEntryRef = null;
            this.currentWriteSetEntry = null;

            this.readSet.Clear();
            this.writeSet.Clear();
            this.commitSet.Clear();
            this.abortSet.Clear();
            this.largestVersionKeySet.Clear();

            // init and get tx id
            this.InitTx();
        }

        internal void SetAbortMsg(string msg)
        {
            //TxAbortReasonTracer.reasons[this.Procedure.pid] = msg;
        }

        internal void AbortWithMessage(string abortMessage)
        {
            this.SetAbortMsg(abortMessage);
            this.CurrentProc = this.abortProc;
            this.CurrentProc();
        }

        internal void AbortWithMessageIfNDebug(string abortMessage)
        {
            this.SetAbortMsg(abortMessage);
            this.CurrentProc = this.abortProc;
            if (!this.DEBUG_MODE)
            {
                this.CurrentProc();
            }
        }


        internal void InitTx()
        {
            this.Progress = TxProgress.Initi;
            long txId = this.txRange.NextTxCandidate();
            this.recycleTxReq.Set(txId, this.remoteTxEntryRef);
            this.recycleTxReq.Use();
            this.versionDb.EnqueueTxEntryRequest(txId, this.recycleTxReq, this.execId);
            this.CurrentProc = this.recycleTxProc;
            this.RecycleTx();
        }

        internal void NewTxId()
        {
            if (!this.newTxIdReq.Finished)
            {
                return;
            }

            while (!(bool)this.newTxIdReq.Result)
            {
                // Retry in loop to get the unique txId
                long newId = this.txRange.NextTxCandidate();
                this.newTxIdReq.Set(newId);
                this.newTxIdReq.Use();
                this.versionDb.EnqueueTxEntryRequest(newId, this.newTxIdReq, this.execId);

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
            // record the remote tx entry
            this.remoteTxEntryRef = this.newTxIdReq.RemoteTxEntry;
            this.newTxIdReq.Free();

            this.inserTxIdReq.Set(txId, this.remoteTxEntryRef);
            this.inserTxIdReq.Use();
            this.versionDb.EnqueueTxEntryRequest(this.txId, this.inserTxIdReq, this.execId);

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

            if (!(bool)this.recycleTxReq.Result)
            {
                this.CurrentProc = this.abortProc;
                this.CurrentProc();
            }

            // Recycled successfully
            this.txId = this.recycleTxReq.TxId;
            // record the remote txEntry
            this.remoteTxEntryRef = this.recycleTxReq.RemoteTxEntry;

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
                if (!this.replaceReq.IsActive() && !this.uploadReq.IsActive() &&
                    !this.getTxReq.IsActive() && !this.retryReplaceReq.IsActive())
                {
                    WriteSetEntry writeEntry = this.writeSet[this.writeSetIndex++];
                    this.currentWriteSetEntry = writeEntry;
                    string tableId = writeEntry.TableId;
                    object recordKey = writeEntry.RecordKey;
                    object payload = writeEntry.Payload;

                    // should check the type of writes, insert/update/delete
                    // The write-set record is an insert or update record, try to insert the new version
                    if (payload != null)
                    {
                        // VersionEntry newImageEntry = TransactionExecutor.versionEntryArray[versionEntryIndex];
                        VersionEntry newImageEntry = this.localUploadVerEntry;
                        newImageEntry.VersionKey = writeEntry.VersionKey;
                        newImageEntry.Record = payload;
                        newImageEntry.TxId = this.txId;
                        newImageEntry.BeginTimestamp = VersionEntry.DEFAULT_BEGIN_TIMESTAMP;
                        newImageEntry.EndTimestamp = VersionEntry.DEFAULT_END_TIMESTAMP;
                        newImageEntry.MaxCommitTs = 0L;

                        this.uploadReq.Set(
                            tableId,
                            recordKey,
                            newImageEntry.VersionKey,
                            newImageEntry,
                            this.txId,
                            writeEntry.RemoteVerList);
                        this.uploadReq.Use();
                        this.versionDb.EnqueueVersionEntryRequest(tableId, this.uploadReq, this.execId);
                    }
                    // The write-set record is a delete record, only replace the old version
                    else
                    {
                        ReadSetEntry readVersion = this.FindReadSetEntry(tableId, recordKey);
                        this.replaceReq.Set(
                            tableId,
                            recordKey,
                            readVersion.VersionKey,
                            readVersion.BeginTimestamp,
                            long.MaxValue,
                            this.txId,
                            VersionEntry.EMPTY_TXID,
                            long.MaxValue,
                            this.localVerEntry,
                            readVersion.RemoteVerEntry);
                        this.replaceReq.Use();

                        this.replaceRemoteVerRef = readVersion.RemoteVerEntry;
                        this.versionDb.EnqueueVersionEntryRequest(tableId, this.replaceReq, this.execId);
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
                        this.AbortWithMessageIfNDebug("Failed to upload the new image");
                        return;
                    }

                    string tableId = uploadReq.TableId;
                    object recordKey = uploadReq.RecordKey;

                    // Add the info to the abortSet
                    this.AddVersionToAbortSet(tableId, recordKey, this.uploadReq.VersionKey,
                        VersionEntry.DEFAULT_BEGIN_TIMESTAMP, VersionEntry.DEFAULT_END_TIMESTAMP,
                        uploadReq.VersionEntry, uploadReq.RemoteVerList);
                    // Add the info to the commitSet
                    this.AddVersionToCommitSet(tableId, recordKey, this.uploadReq.VersionKey,
                        TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP, long.MaxValue,
                        uploadReq.VersionEntry, uploadReq.RemoteVerList);

                    this.localUploadVerEntry = this.uploadReq.RemoteVerEntry;

                    this.uploadReq.Free();

                    ReadSetEntry readVersion = this.FindReadSetEntry(tableId, recordKey);
                    if (readVersion == null)
                    {
                        // The write-set record is an insert record. 
                        // Moves to the next write-set record or the next phase.
                        continue;
                    }

                    this.replaceRemoteVerRef = readVersion.RemoteVerEntry;

                    // Initiates a new request to append the new image to the tail of the version list.
                    // The tail entry could be [Ts, inf, -1], [Ts, inf, txId1] or [-1, -1, txId1].
                    // The first case indicates that no concurrent tx is locking the tail.
                    // The second case indicates that one concurrent tx is holding the tail. 
                    // The third case means that a concurrent tx is creating a new tail, which was seen by this tx. 
                    this.replaceReq.Set(
                        tableId,
                        recordKey,
                        readVersion.VersionKey,
                        readVersion.BeginTimestamp,
                        long.MaxValue,
                        this.txId,
                        VersionEntry.EMPTY_TXID,
                        long.MaxValue,
                        this.localVerEntry,
                        readVersion.RemoteVerEntry);
                    this.replaceReq.Use();
                    this.versionDb.EnqueueVersionEntryRequest(tableId, this.replaceReq, this.execId);
                }
                else if (this.replaceReq.IsActive() && !this.uploadReq.IsActive() &&
                    !this.retryReplaceReq.IsActive() && !this.getTxReq.IsActive())
                {
                    if (!this.replaceReq.Finished)
                    {
                        // The prior request hasn't been processed. Returns the control to the caller.
                        return;
                    }

                    this.replaceVerEntry = this.replaceReq.Result as VersionEntry;
                    this.replaceTableId = this.replaceReq.TableId;
                    this.replaceReq.Free();

                    if (this.localVerEntry == null)
                    {
                        this.AbortWithMessageIfNDebug("Version Entry null");
                        return;
                    }

                    long rolledBackBegin = this.replaceVerEntry.BeginTimestamp;
                    this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, this.replaceVerEntry.MaxCommitTs);

                    if (this.replaceVerEntry.TxId == this.txId) // Appending to the tail was successful
                    {
                        // Add the updated tail to the abort set
                        this.AddVersionToAbortSet(
                            this.replaceTableId,
                            this.currentWriteSetEntry.RecordKey,
                            this.replaceVerEntry.VersionKey,
                            rolledBackBegin,
                            long.MaxValue,
                            this.replaceRemoteVerRef);
                        // Add the updated tail to the commit set
                        this.AddVersionToCommitSet(
                            this.replaceTableId,
                            this.currentWriteSetEntry.RecordKey,
                            this.replaceVerEntry.VersionKey,
                            rolledBackBegin,
                            TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP,
                            this.replaceRemoteVerRef);

                        // Move on to the next write-set record or the next phase
                        continue;
                    }
                    else if (this.replaceVerEntry.TxId >= 0)
                    {
                        // The first try was unsuccessful because the tail is hold by another concurrent tx. 
                        // If the concurrent tx has finished (committed or aborted), there is a chance for this tx
                        // to re-gain the lock. 
                        // Enqueues a request to check the status of the tx that is holding the tail.
                        this.getTxReq.Set(this.replaceVerEntry.TxId, this.txId, this.localTxEntry, this.remoteTxEntryRef);
                        this.getTxReq.Use();
                        this.versionDb.EnqueueTxEntryRequest(this.replaceVerEntry.TxId, this.getTxReq, this.execId);

                        continue;
                    }
                    else
                    {
                        // The new version is failed to append to the tail of the version list, 
                        // because the old tail seen by this tx is not the tail anymore

                        this.AbortWithMessageIfNDebug("Failed to append the tail version");
                        return;
                    }
                }
                else if (this.getTxReq.IsActive() && !this.replaceReq.IsActive() &&
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
                        this.AbortWithMessageIfNDebug("conflict tx status Ongoing");
                        return;
                    }

                    // The new tail was created by a concurrent tx, yet has not been post-processed. 
                    // The current tx tries to update the tail to the post-processing image and obtain the lock.
                    if (this.replaceVerEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP)
                    {
                        // Only if a new tail's owner tx has been committed, can it be seen by 
                        // the current tx, who is trying to gain the lock of the tail. 
                        Debug.Assert(conflictTxStatus.Status == TxStatus.Committed);

                        this.retryReplaceReq.Set(
                            this.replaceTableId,
                            this.currentWriteSetEntry.RecordKey,
                            replaceVerEntry.VersionKey,
                            conflictTxStatus.CommitTime,
                            long.MaxValue,
                            this.txId,
                            conflictTxStatus.TxId,
                            VersionEntry.DEFAULT_END_TIMESTAMP,
                            this.localVerEntry,
                            this.replaceRemoteVerRef);
                        this.retryReplaceReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(this.replaceTableId, this.retryReplaceReq, this.execId);
                    }
                    // The old tail was locked by a concurrent tx, which has finished but has not released the lock. 
                    // The current lock tries to replace the lock's owner to itself. 
                    else if (this.replaceVerEntry.EndTimestamp == long.MaxValue)
                    {
                        // The owner tx of the lock has committed. This version entry is not the tail anymore.
                        if (conflictTxStatus.Status == TxStatus.Committed)
                        {
                            this.AbortWithMessageIfNDebug("the owner tx of the lock committed");
                            return;
                        }
                        else if (conflictTxStatus.Status == TxStatus.Aborted)
                        {
                            this.retryReplaceReq.Set(
                                this.replaceTableId,
                                this.currentWriteSetEntry.RecordKey,
                                this.replaceVerEntry.VersionKey,
                                this.replaceVerEntry.BeginTimestamp,
                                long.MaxValue,
                                this.txId,
                                conflictTxStatus.TxId,
                                long.MaxValue,
                                this.localVerEntry,
                                this.replaceRemoteVerRef);

                            this.retryReplaceReq.Use();
                            this.versionDb.EnqueueVersionEntryRequest(this.replaceTableId, this.retryReplaceReq, this.execId);
                        }
                    }
                }
                else if (this.retryReplaceReq.IsActive() && !this.replaceReq.IsActive() &&
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
                        this.AbortWithMessageIfNDebug("retry entry null...");
                        return;
                    }

                    long rolledBackBegin = retryEntry.BeginTimestamp;
                    this.maxCommitTsOfWrites = Math.Max(this.maxCommitTsOfWrites, retryEntry.MaxCommitTs);

                    // Add the updated tail to the abort set
                    this.AddVersionToAbortSet(
                        this.replaceTableId,
                        this.currentWriteSetEntry.RecordKey,
                        retryEntry.VersionKey,
                        rolledBackBegin,
                        long.MaxValue,
                        this.replaceRemoteVerRef);

                    // Add the updated tail to the commit set
                    this.AddVersionToCommitSet(
                        this.replaceTableId,
                        this.currentWriteSetEntry.RecordKey,
                        retryEntry.VersionKey,
                        rolledBackBegin,
                        TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP,
                        this.replaceRemoteVerRef);

                    this.retryReplaceReq.Free();
                }
                else
                {
                    throw new TransactionException("An illegal state of the uploading phase.");
                }
            }

            this.SetCommitTimestamp();
            // when there is no write, I think we could bypass the follow-up steps for read-only transaction
            /*
            if (this.writeSet.Count > 0)
            {
                this.SetCommitTimestamp();
            }
            else
            {
                this.WriteToLog();
            }
            */
            
        }

        private void AddVersionToAbortSet(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTs,
            long endTs,
            VersionEntry remoteVerEntryRef = null,
            IDictionary<long, VersionEntry> remoteVerList = null)
        {
            this.abortSet.AllocateNew().Set(
                tableId, recordKey, versionKey, beginTs, endTs,
                remoteVerEntryRef, remoteVerList);
        }

        private void AddVersionToCommitSet(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTs,
            long endTs,
            VersionEntry remoteVerEntryRef = null,
            IDictionary<long, VersionEntry> remoteVerList = null)
        {
            this.commitSet.AllocateNew().Set(
                tableId, recordKey, versionKey, beginTs, endTs,
                remoteVerEntryRef, remoteVerList);
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

            this.commitTsReq.Set(this.txId, this.commitTs, this.remoteTxEntryRef);
            this.commitTsReq.Use();
            this.versionDb.EnqueueTxEntryRequest(this.txId, this.commitTsReq, this.execId);

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
                this.AbortWithMessageIfNDebug("commit time < 0");
                return;
            }
            else
            {
                this.commitTs = commitTime;
                this.CurrentProc = this.validateProc;
                this.rereadVerEntry = null;
                if (!this.DEBUG_MODE)
                {
                    this.CurrentProc();
                }
                return;
            }
        }

        internal void Validate()
        {
            // 1. Case 1: readSet is not empty, rereadVerEntry = null, requests aren't active
            //            should validate the next version
            // 2. Case 2: One of those requests is active, handle the returned request
            // 3. Case 3: All requests are not active, handle the rereadVersionEntry
            while (this.readSet.Count > 0 || this.rereadVerEntry != null ||
                this.readReq.IsActive() || this.getTxReq.IsActive() ||
                this.updateMaxTsReq.IsActive() || this.updateBoundReq.IsActive())
            {
                // no pending requests or versions, validate new version
                if (!this.readReq.IsActive() && !this.getTxReq.IsActive() &&
                    !this.updateMaxTsReq.IsActive() && !this.updateBoundReq.IsActive()
                    && this.rereadVerEntry == null)
                {
                    ReadSetEntry entry = this.readSet.Pop();
                    if (this.FindWriteSetEntry(entry.TableId, entry.RecordKey) != null)
                    {
                        continue;
                    }

                    this.readReq.Set(
                        entry.TableId,
                        entry.RecordKey,
                        entry.VersionKey,
                        this.txId,
                        this.localVerEntry,
                        entry.RemoteVerEntry);
                    this.readReq.Use();

                    this.versionDb.EnqueueVersionEntryRequest(entry.TableId, readReq, this.execId);
                }
                // the readReq is not null, handle the pending request and check
                else if (this.readReq.IsActive() && !this.getTxReq.IsActive()
                    && !this.updateMaxTsReq.IsActive() && !this.updateBoundReq.IsActive())
                {
                    if (!this.readReq.Finished)
                    {
                        return;
                    }

                    this.rereadVerEntry = this.readReq.Result as VersionEntry;
                    string tableId = this.readReq.TableId;
                    this.readReq.Free();

                    if (this.rereadVerEntry == null)
                    {
                        this.AbortWithMessageIfNDebug("read entry null");
                        return;
                    }

                    // Send requests to update the maxVersionCommits
                    if (this.rereadVerEntry.MaxCommitTs < this.commitTs)
                    {
                        // Updates the version's max commit timestamp
                        this.updateMaxTsReq.Set(
                            tableId,
                            this.readReq.RecordKey,
                            this.rereadVerEntry.VersionKey,
                            this.commitTs,
                            this.txId,
                            this.localVerEntry,
                            this.readReq.RemoteVerEntry);
                        this.updateMaxTsReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(tableId, updateMaxTsReq, this.execId);
                    }

                    // otherwise, re-enter the loop and it will be catched by this.rereadVerEntry != null
                }
                else if (this.updateMaxTsReq.IsActive() && !this.readReq.IsActive() &&
                    !this.getTxReq.IsActive() && !this.updateBoundReq.IsActive())
                {
                    if (!this.updateMaxTsReq.Finished)
                    {
                        return;
                    }

                    this.rereadVerEntry = updateMaxTsReq.Result as VersionEntry;
                    this.updateMaxTsReq.Free();
                    if (this.rereadVerEntry == null)
                    {
                        this.AbortWithMessageIfNDebug("read entry null: update Max Ts Req");
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
                    TxTableEntry remoteTxEntry = this.getTxReq.RemoteTxEntry;
                    this.getTxReq.Free();

                    if (txEntry == null)
                    {
                        this.AbortWithMessageIfNDebug("tx table entry null");
                        return;
                    }

                    if (txEntry.Status == TxStatus.Aborted)
                    {
                        // The tx holding the version has been aborted. Validation passed.
                        this.rereadVerEntry = null;
                        continue;
                    }
                    else if (txEntry.Status == TxStatus.Committed || txEntry.CommitTime >= 0)
                    {
                        if (this.commitTs > txEntry.CommitTime)
                        {
                            this.AbortWithMessageIfNDebug("this.commitTs > txEntry.CommitTime");
                            return;
                        }
                        else
                        {
                            // pass the validation
                            this.rereadVerEntry = null;
                            continue;
                        }
                    }
                    else
                    {
                        this.updateBoundReq.Set(txEntry.TxId, this.txId, this.commitTs + 1, remoteTxEntry);
                        this.updateBoundReq.Use();

                        this.versionDb.EnqueueTxEntryRequest(txEntry.TxId, this.updateBoundReq, this.execId);
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
                        this.AbortWithMessageIfNDebug("txCommitTs == VersionDb.RETURN_ERROR_CODE");
                        return;
                    }
                    else if (txCommitTs == TxTableEntry.DEFAULT_COMMIT_TIME)
                    {
                        // The tx who is locking the version has not had its commit timestamp.
                        this.rereadVerEntry = null;
                        continue;
                    }
                    else if (this.commitTs > txCommitTs)
                    {
                        this.AbortWithMessageIfNDebug("this.commitTs > txCommitTs");
                        return;
                    }
                    else if (this.commitTs <= txCommitTs)
                    {
                        // pass the validation
                        this.rereadVerEntry = null;
                        continue;
                    }
                }
                // Here we put checking rereadVerEntry as the last condition
                // During the checking process, there may have the case rereadVerEntry != null
                // and one of those requests is active. In this case, we should process the requests
                // at first and they will be caught in those 4 previous cases.
                else if (this.rereadVerEntry != null)
                {
                    // Here it could be confirmed that VersionEntry's MaxCommitTs must be larger or equal this.commitTs
                    Debug.Assert(this.rereadVerEntry.MaxCommitTs >= this.commitTs);

                    // No need to update the version's maxCommitTs.
                    // Check whether or not the re-read version is occupied by another tx.
                    if (this.rereadVerEntry.TxId != VersionEntry.EMPTY_TXID)
                    {
                        // A concurrent tx is locking the version. Checks the tx's status to decide how to move forward, 
                        // i.e., abort or pass validation.

                        // reread other versions, no remote tx table entry reference
                        this.getTxReq.Set(this.rereadVerEntry.TxId, this.txId, this.localTxEntry, this.remoteTxEntryRef);
                        this.getTxReq.Use();
                        this.versionDb.EnqueueTxEntryRequest(this.rereadVerEntry.TxId, this.getTxReq, this.execId);
                    }
                    else
                    {
                        if (this.commitTs > this.rereadVerEntry.EndTimestamp)
                        {
                            // A new version has been created before this tx can commit.
                            // Abort the tx.
                            this.AbortWithMessageIfNDebug("a new version has been created before this commit");
                            return;
                        }
                        else
                        {
                            // No new version has bee created. This record passes validation. 
                            this.rereadVerEntry = null;
                            continue;
                        }
                    }
                }
            }
            this.WriteToLog();
        }

        internal void WriteToLog()
        {
            this.updateTxReq.Set(this.txId, TxStatus.Committed, this.remoteTxEntryRef);
            this.updateTxReq.Use();

            this.versionDb.EnqueueTxEntryRequest(this.txId, this.updateTxReq, this.execId);
            this.CurrentProc = this.updateTxCommitProc;
            this.CurrentProc();
        }

        internal void UpdateTxStatusToCommited()
        {
            if (!this.updateTxReq.Finished)
            {
                return;
            }

            this.updateTxReq.Free();
            this.TxStatus = TxStatus.Committed;
            this.CurrentProc = this.commitPostproProc;
            this.CurrentProc();
        }

        internal void PostProcessingAfterCommit()
        {
            while (this.commitSet.Count > 0 || this.replaceReq.IsActive())
            {
                if (!this.replaceReq.IsActive())
                {
                    PostProcessingEntry entry = this.commitSet.Pop();
                    if (entry.BeginTimestamp == TransactionExecution.UNSET_TX_COMMIT_TIMESTAMP)
                    {
                        this.replaceReq.Set(
                            entry.TableId,
                            entry.RecordKey,
                            entry.VersionKey,
                            this.commitTs,
                            entry.EndTimestamp,
                            VersionEntry.EMPTY_TXID,
                            this.txId,
                            VersionEntry.DEFAULT_END_TIMESTAMP,
                            this.localVerEntry,
                            entry.RemoteVerEntry);

                        this.replaceReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.replaceReq, this.execId);
                    }
                    else
                    {
                        this.replaceReq.Set(
                            entry.TableId,
                            entry.RecordKey,
                            entry.VersionKey,
                            entry.BeginTimestamp,
                            this.commitTs,
                            VersionEntry.EMPTY_TXID,
                            this.txId,
                            long.MaxValue,
                            this.localVerEntry,
                            entry.RemoteVerEntry);
                        this.replaceReq.Use();
                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.replaceReq, this.execId);

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
            // All post-processing records have been uploaded.
            this.Progress = TxProgress.Close;
            this.CurrentProc = null;
            return;
        }

        internal void PostProcessingAfterAbort()
        {
            while (this.abortSet.Count > 0 || this.replaceReq.IsActive() || this.deleteReq.IsActive())
            {
                if (!this.replaceReq.IsActive() && !this.deleteReq.IsActive())
                {
                    PostProcessingEntry entry = this.abortSet.Pop();
                    if (entry.BeginTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP)
                    {
                        this.deleteReq.Set(entry.TableId, entry.RecordKey, entry.VersionKey, this.txId, entry.RemoteVerList);
                        this.deleteReq.Use();

                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.deleteReq, this.execId);
                    }
                    else
                    {
                        this.replaceReq.Set(
                            entry.TableId,
                            entry.RecordKey,
                            entry.VersionKey,
                            entry.BeginTimestamp,
                            entry.EndTimestamp,
                            VersionEntry.EMPTY_TXID,
                            this.txId,
                            long.MaxValue,
                            this.localVerEntry,
                            entry.RemoteVerEntry);

                        this.replaceReq.Use();
                        this.versionDb.EnqueueVersionEntryRequest(entry.TableId, this.replaceReq, this.execId);
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

            // All pending records have been reverted.
            this.Progress = TxProgress.Close;
            this.CurrentProc = null;
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
            this.TxStatus = TxStatus.Aborted;
            this.Progress = TxProgress.Final;
            this.replaceReq.Free();
            this.updateTxReq.Set(this.txId, TxStatus.Aborted, this.remoteTxEntryRef);
            this.updateTxReq.Use();
            this.versionDb.EnqueueTxEntryRequest(this.txId, updateTxReq, this.execId);

            this.CurrentProc = this.updateTxAbortProc;
            if (!this.DEBUG_MODE)
            {
                this.CurrentProc();
            }
        }

        internal void UpdateTxStatusToAborted()
        {
            if (!this.updateTxReq.Finished)
            {
                return;
            }

            this.CurrentProc = this.abortPostproProc;
            if (!this.DEBUG_MODE)
            {
                this.CurrentProc();
            }
            return;
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
                    this.AbortWithMessage("write set tableid recordkey null");
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
                this.AbortWithMessage("record is already in the local read set");
                //throw new TransactionException("The same record already exists.");
            }
            // Neither the readSet and writeSet have the recordKey
            else
            {
                VersionKeyEntry versionKeyEntry = this.FindVersionKeyEntry(tableId, recordKey);

                this.writeSet.AllocateNew().Set(tableId, recordKey, record,
                    versionKeyEntry.VersionKey + 1, versionKeyEntry.RemoteVerList);
            }
            this.Procedure?.InsertCallBack(tableId, recordKey, record);
        }

        public void InitAndInsert(
            string tableId, object recordKey, object record)
        {
            bool dummy1;
            object dummy2;
            ReadAndInitialize(tableId, recordKey, out dummy1, out dummy2);
            Insert(tableId, recordKey, record);
        }

        public void Read(string tableId, object recordKey, out bool received, out object payload)
        {
            this.ReadPayload = null;
            this.Read(tableId, recordKey, false, out received, out payload);
        }

        public object SyncRead(string tableId, object recordKey)
        {
            bool dummy1;
            object dummy2;
            this.ReadPayload = null;
            // Assume this Read never block and return before getting result
            this.Read(tableId, recordKey, false, out dummy1, out dummy2);
            return this.ReadPayload;
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
                    this.AbortWithMessage("record has been deleted by this tx");
                    //throw new TransactionException("The record to be updated has been deleted.");
                }
            }
            else if (readEntry != null)
            {
                this.writeSet.AllocateNew().Set(
                    tableId, recordKey, payload, readEntry.TailKey + 1,
                    readEntry.RemoteVerList);
            }
            else
            {
                this.AbortWithMessage("update fail, some reason");
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
                    this.AbortWithMessage("delete fail reason1");
                    // throw new TransactionException("The record to be deleted has been deleted by the same tx.");
                }
            }
            else if (readEntry != null)
            {
                payload = readEntry.Record;

                this.writeSet.AllocateNew().Set(
                    tableId, recordKey, null, -1, readEntry.RemoteVerList);
            }
            else
            {
                this.AbortWithMessage("delete fail reason2");
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
            this.readLargestVersionKey = VersionEntry.VERSION_KEY_START_INDEX;
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
                this.initiGetVListReq.Set(tableId, recordKey, this.txId);
                this.initiGetVListReq.Use();

                this.versionDb.EnqueueVersionEntryRequest(tableId, this.initiGetVListReq, this.execId);
            }
            else
            {
                this.getVListReq.Set(tableId, recordKey, this.txId, this.versionList, this.remoteVersionRefList);
                this.getVListReq.Use();

                this.versionDb.EnqueueVersionEntryRequest(tableId, this.getVListReq, this.execId);
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
                this.remoteVerListRef = this.getVListReq.RemoteVerList;

                this.getVListReq.Free();

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
                if ((bool)this.initiGetVListReq.Result)
                {
                    this.largestVersionKeySet.AllocateNew().Set(
                        this.readTableId,
                        this.readRecordKey,
                        VersionEntry.VERSION_KEY_START_INDEX,
                        this.initiGetVListReq.RemoteVerList);

                    this.remoteVerListRef = this.initiGetVListReq.RemoteVerList;
                    this.initiGetVListReq.Free();
                    // No read call back is invoked. 
                    this.Progress = TxProgress.Open;
                    this.CurrentProc = null;
                    this.Procedure?.ReadCallback(this.readTableId, this.readRecordKey, null);
                    return;
                }
                else
                {
                    this.getVListReq.Set(this.readTableId, this.readRecordKey, this.txId, this.versionList, this.remoteVersionRefList);
                    this.getVListReq.Use();

                    this.versionDb.EnqueueVersionEntryRequest(this.readTableId, this.getVListReq, this.execId);

                    this.CurrentProc = this.readVersionListProc;
                    this.CurrentProc();
                }
            }
        }

        private void SortVersionList()
        {
            this.versionList.Sort(this.readEntryCount);
            if (this.versionDb is SingletonVersionDb)
            {
                this.remoteVersionRefList.Sort(this.readEntryCount);
            }
        }

        /// <summary>
        /// (only) depends on `this.versionList`, `this.readEntryCount`
        /// </summary>
        /// <param name="index">
        /// output index of the visible version in this.versionList, invalid when no visible version found
        /// </param>
        /// <param name="maxVisibleVersion">
        /// output largest versionKey in this.versionList, valid even when no visible version found
        /// </param>
        /// <returns> returns `null` if no visible version</returns>
        private VersionEntry PickVisibleVersion(out int index, out long maxVisibleVersion)
        {
            maxVisibleVersion = VersionEntry.VERSION_KEY_START_INDEX;
            index = this.readEntryCount - 1;
            for (; index >= 0; --index)
            {
                VersionEntry versionEntry = this.versionList[index];
                // Dirty version
                if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP)
                {
                    CheckInvariant(versionEntry.BeginTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP);
                    CheckInvariant(index == this.readEntryCount - 1);
                    continue;
                }
                // Dirty version doesn't count as maxVersionKey
                maxVisibleVersion = Math.Max(maxVisibleVersion, versionEntry.VersionKey);
                // Current newest version
                if (versionEntry.EndTimestamp == long.MaxValue)
                {
                    CheckInvariant(versionEntry.BeginTimestamp >= 0);
                    return versionEntry;
                }
                // else: 0 <= versionEntry.EndTimestamp < Infinity
                // an old version modified by someone else, and its following
                // versions are sure to be invisible too
                CheckInvariant(versionEntry.TxId == VersionEntry.EMPTY_TXID);
                break;
            }
            return null;
        }

        internal void ReadCheckVersionEntry()
        {
            SortVersionList();
            // VersionEntry visibleVersion = null;
            // Keep a committed version to retrieve the largest version key
            // VersionEntry committedVersion = null;
            // TxTableEntry pendingTx = null;
            int visibleVersionIdx;
            VersionEntry visibleVersion = this.PickVisibleVersion(
                out visibleVersionIdx, out this.readLargestVersionKey);

            VersionEntry visiableVersionRef = null;
            if (visibleVersion != null && this.versionDb is SingletonVersionDb)
            {
                visiableVersionRef = this.remoteVersionRefList[visibleVersionIdx];
                CheckInvariant(visiableVersionRef.VersionKey == visibleVersion.VersionKey);
            }

            // while (this.readEntryCount > 0)
            // {
            //     VersionEntry versionEntry = this.versionList[this.readEntryCount - 1];
            //     // Wait for the GetTxEntry response
            //     if (this.getTxReq.IsActive())
            //     {
            //         if (!this.getTxReq.Finished)
            //         {
            //             return;
            //         }

            //         pendingTx = this.getTxReq.Result as TxTableEntry;
            //         this.getTxReq.Free();

            //         --this.readEntryCount;

            //         if (pendingTx == null)
            //         {
            //             // Failed to retrieve the status of the tx holding the version. 
            //             // Moves on to the next version.
            //             continue;
            //         }

            //         // The current version is commited and should be extracted the largest version key
            //         committedVersion = versionEntry;

            //         this.readLargestVersionKey = Math.Max(versionEntry.VersionKey, this.readLargestVersionKey);

            //         if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP)
            //         {
            //             // this version is a dirty version
            //             if (pendingTx.Status != TxStatus.Committed)
            //             {
            //                 continue;
            //             }
            //             // this version is a committed version but doesn't finish post-processing
            //             visibleVersion = new VersionEntry(
            //                 versionEntry.RecordKey,
            //                 versionEntry.VersionKey,
            //                 pendingTx.CommitTime,
            //                 long.MaxValue,
            //                 versionEntry.Record,
            //                 VersionEntry.EMPTY_TXID,
            //                 versionEntry.MaxCommitTs);
            //         }
            //         else if (versionEntry.EndTimestamp == long.MaxValue)
            //         {
            //             // A dirty write has been appended after this version entry. 
            //             // This version is visible if the writing tx has not been committed
            //             if (pendingTx.Status != TxStatus.Committed)
            //             {
            //                 visibleVersion = versionEntry;
            //             }
            //             else
            //             {
            //                 AbortWithMessage("This version is no longer visible");
            //                 return;
            //             }
            //         }
            //         else
            //         {
            //             throw new Exception("Invalid state: 0 <= EndTimestamp < Inf");
            //         }
            //     }
            //     else
            //     {
            //         if (versionEntry.EndTimestamp == VersionEntry.DEFAULT_END_TIMESTAMP)
            //         {
            //             Debug.Assert(versionEntry.BeginTimestamp == VersionEntry.DEFAULT_BEGIN_TIMESTAMP);
            //             --this.readEntryCount;
            //             continue;
            //         }
            //         if (versionEntry.EndTimestamp == long.MaxValue)
            //         {
            //             --this.readEntryCount;
            //             visibleVersion = versionEntry;
            //         }
            //         else if (versionEntry.TxId >= 0)
            //         {
            //             if (pendingTx != null && pendingTx.TxId == versionEntry.TxId)
            //             {
            //                 this.getTxReq.Result = pendingTx;
            //                 this.getTxReq.Use();
            //                 this.getTxReq.Finished = true;
            //             }
            //             else
            //             {
            //                 // Send the GetTxEntry request
            //                 this.getTxReq.Set(versionEntry.TxId, this.txId, this.localTxEntry, this.remoteTxEntryRef);
            //                 this.getTxReq.Use();

            //                 this.versionDb.EnqueueTxEntryRequest(versionEntry.TxId, getTxReq, this.execId);
            //             }
            //         }
            //         else
            //         {
            //             committedVersion = versionEntry;
            //             this.readLargestVersionKey = Math.Max(versionEntry.VersionKey, this.readLargestVersionKey);
            //             --this.readEntryCount;
            //             // When a tx has a begin timestamp after intialization.
            //             // this.beginTimestamp is currently not used. It's used
            //             // while we may want to retrieve a snapshot of a version
            //             // list in the future.
            //             if (this.beginTimestamp >= 0 &&
            //                 this.beginTimestamp >= versionEntry.BeginTimestamp &&
            //                 this.beginTimestamp < versionEntry.EndTimestamp)
            //             {

            //                 visibleVersion = versionEntry;
            //             }
            //             // When a tx has no begin timestamp after intialization, the tx is under serializability. 
            //             // A read always returns the most-recently committed version.
            //             else if (versionEntry.EndTimestamp == long.MaxValue)
            //             {
            //                 visibleVersion = versionEntry;
            //             }
            //             else if (versionEntry.EndTimestamp != VersionEntry.DEFAULT_END_TIMESTAMP)
            //             {
            //                 // caused by a delete operation or concurrent read
            //                 break;
            //             }
            //             else
            //             {
            //                 throw new Exception("EndTimestamp == DEFAULT and TxId == EMPTY");
            //             }
            //         }
            //     }

            //     // Break the loop once find a visiable version
            //     if (visibleVersion != null)
            //     {
            //         // save the reference of visiable version entry
            //         // JUST FOR IN-MEMORY VERSION

            //         if (this.remoteVersionRefList.Count > this.readEntryCount)
            //         {
            //         visiableVersionRef = this.remoteVersionRefList[this.readEntryCount];
            //         Debug.Assert(visiableVersionRef != null);
            //         }
            //         break;
            //     }
            // }

            object payload = null;
            // Put the visible version into the read set. 
            if (visibleVersion != null)
            {
                payload = visibleVersion.Record;
                this.ReadPayload = payload;

                this.readSet.AllocateNew().Set(
                        this.readTableId,
                        this.readRecordKey,
                        visibleVersion.VersionKey,
                        visibleVersion.BeginTimestamp,
                        visibleVersion.EndTimestamp,
                        visibleVersion.TxId,
                        visibleVersion.Record,
                        this.readLargestVersionKey,
                        visiableVersionRef,
                        this.remoteVerListRef);
            }
            else
            {
                // Store the largest version key

                this.largestVersionKeySet.AllocateNew().Set(
                    this.readTableId,
                    this.readRecordKey,
                    this.readLargestVersionKey,
                    this.remoteVerListRef);
            }

            // Clear the remote version list, since those entries are added into the list rather replaceing
            this.remoteVersionRefList.Clear();
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

        private TxSetEntry.EqualPredicate setEntryEqual =
            new TxSetEntry.EqualPredicate();

        private ReadSetEntry FindReadSetEntry(string tableId, object recordKey)
        {
            return this.readSet.Find(
                this.setEntryEqual.Get(tableId, recordKey));
        }

        private WriteSetEntry
        FindWriteSetEntry(string tableId, object recordKey)
        {
            return this.writeSet.Find(
                this.setEntryEqual.Get(tableId, recordKey));
        }

        private VersionKeyEntry
        FindVersionKeyEntry(string tableId, object recordKey)
        {
            return this.largestVersionKeySet.Find(
                this.setEntryEqual.Get(tableId, recordKey));
        }

        static private void CheckInvariant(bool invariant)
        {
            if (!invariant)
            {
                throw new Exception();
            }
        }
    }
}
