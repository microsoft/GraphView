
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using NonBlocking;

    internal interface IResource
    {
        void Use();
        bool IsActive();
        void Free();
    }

    internal class ResourcePool<T> where T : IResource
    {
        Queue<T> resourcePool;

        public ResourcePool(int capacity)
        {
            this.resourcePool = new Queue<T>(capacity);
        }

        internal T GetResource()
        {
            int count = 0, size = this.resourcePool.Count;
            while (count < size)
            {
                count++;
                T resource = this.resourcePool.Dequeue();
                this.resourcePool.Enqueue(resource);

                if (!resource.IsActive())
                {
                    // use the resource and return
                    resource.Use();
                    return resource;
                }
            }

            return default(T);
        }

        internal void Recycle(T resource)
        {
            resource.Free();
        }

        internal void AddNewResource(T resource)
        {
            this.resourcePool.Enqueue(resource);
        } 
    }

    internal class TxResourceManager
    {
        internal static readonly int workingsetCapacity = 100;

        // Version entry requests
        private readonly ResourcePool<GetVersionListRequest> getVersionListRequests;
        private readonly ResourcePool<InitiGetVersionListRequest> initiGetVersionListRequests;
        private readonly ResourcePool<ReadVersionRequest> readVersionRequests;
        private readonly ResourcePool<ReplaceVersionRequest> replaceVersionRequests;
        private readonly ResourcePool<ReplaceWholeVersionRequest> replaceWholeVersionRequests;
        private readonly ResourcePool<DeleteVersionRequest> deleteVersionRequests;
        private readonly ResourcePool<UpdateVersionMaxCommitTsRequest> updateVersionMaxCommitTsRequests;
        private readonly ResourcePool<UploadVersionRequest> uploadVersionRequests;

        // Tx entry requests
        private readonly ResourcePool<GetTxEntryRequest> getTxEntryRequests;
        private readonly ResourcePool<InsertTxIdRequest> inserTxRequests;
        private readonly ResourcePool<NewTxIdRequest> newTxRequests;
        private readonly ResourcePool<UpdateCommitLowerBoundRequest> updateCommitLowerBoundRequests;
        private readonly ResourcePool<SetCommitTsRequest> setCommitTsRequests;
        private readonly ResourcePool<RecycleTxRequest> recycleTxRequests;
        private readonly ResourcePool<UpdateTxStatusRequest> updateTxStatusRequests;
        private readonly ResourcePool<RemoveTxRequest> removeTxRequests;

        // Entry Resource
        private readonly ResourcePool<ReadSetEntry> readSetEntries;
        private readonly ResourcePool<PostProcessingEntry> postprocessingEntries;
        private readonly ResourcePool<WriteSetEntry> writeSetEntries;
        private readonly ResourcePool<VersionKeyEntry> versionKeyEntries;

        private readonly Queue<TxTableEntry> txTableEntries;
        private readonly Queue<VersionEntry> versionEntries;

        private ResourcePool<TransactionRequest> transRequests;

        public TxResourceManager()
        {
            // Version Entry Requests
            this.getVersionListRequests = new ResourcePool<GetVersionListRequest>(TxResourceManager.workingsetCapacity);
            this.initiGetVersionListRequests = new ResourcePool<InitiGetVersionListRequest>(TxResourceManager.workingsetCapacity);
            this.readVersionRequests = new ResourcePool<ReadVersionRequest>(TxResourceManager.workingsetCapacity);
            this.replaceVersionRequests = new ResourcePool<ReplaceVersionRequest>(TxResourceManager.workingsetCapacity);
            this.replaceWholeVersionRequests = new ResourcePool<ReplaceWholeVersionRequest>(TxResourceManager.workingsetCapacity);
            this.deleteVersionRequests = new ResourcePool<DeleteVersionRequest>(TxResourceManager.workingsetCapacity);
            this.updateVersionMaxCommitTsRequests = new ResourcePool<UpdateVersionMaxCommitTsRequest>(TxResourceManager.workingsetCapacity);
            this.uploadVersionRequests = new ResourcePool<UploadVersionRequest>(TxResourceManager.workingsetCapacity);


            // TxEntry Requests
            this.getTxEntryRequests = new ResourcePool<GetTxEntryRequest>(TxResourceManager.workingsetCapacity);
            this.inserTxRequests = new ResourcePool<InsertTxIdRequest>(TxResourceManager.workingsetCapacity);
            this.newTxRequests = new ResourcePool<NewTxIdRequest>(TxResourceManager.workingsetCapacity);
            this.updateCommitLowerBoundRequests = 
                new ResourcePool<UpdateCommitLowerBoundRequest>(TxResourceManager.workingsetCapacity);
            this.setCommitTsRequests = new ResourcePool<SetCommitTsRequest>(TxResourceManager.workingsetCapacity);
            this.recycleTxRequests = new ResourcePool<RecycleTxRequest>(TxResourceManager.workingsetCapacity);
            this.updateTxStatusRequests = new ResourcePool<UpdateTxStatusRequest>(TxResourceManager.workingsetCapacity);
            this.removeTxRequests = new ResourcePool<RemoveTxRequest>(TxResourceManager.workingsetCapacity);

            this.readSetEntries = new ResourcePool<ReadSetEntry>(TxResourceManager.workingsetCapacity);
            this.postprocessingEntries = new ResourcePool<PostProcessingEntry>(TxResourceManager.workingsetCapacity);
            this.writeSetEntries = new ResourcePool<WriteSetEntry>(TxResourceManager.workingsetCapacity);
            this.versionKeyEntries = new ResourcePool<VersionKeyEntry>(TxResourceManager.workingsetCapacity);

            this.txTableEntries = new Queue<TxTableEntry>();
            this.versionEntries = new Queue<VersionEntry>();

            this.transRequests = new ResourcePool<TransactionRequest>(TxResourceManager.workingsetCapacity);

            for (int i = 0; i < TxResourceManager.workingsetCapacity; i++)
            {
                this.getVersionListRequests.AddNewResource(new GetVersionListRequest(null, null, null));
                this.initiGetVersionListRequests.AddNewResource(new InitiGetVersionListRequest(null, null, null));
                this.readVersionRequests.AddNewResource(new ReadVersionRequest(null, null, -1));
                this.replaceVersionRequests.AddNewResource(new ReplaceVersionRequest(null, null, -1, -1, -1, -1, -1, -1));
                this.replaceWholeVersionRequests.AddNewResource(new ReplaceWholeVersionRequest(null, null, -1, null));
                this.deleteVersionRequests.AddNewResource(new DeleteVersionRequest(null, null, -1));
                this.updateVersionMaxCommitTsRequests.AddNewResource(new UpdateVersionMaxCommitTsRequest(null, null, -1, -1));
                this.uploadVersionRequests.AddNewResource(new UploadVersionRequest(null, null, -1, null));

                this.getTxEntryRequests.AddNewResource(new GetTxEntryRequest(-1));
                this.inserTxRequests.AddNewResource(new InsertTxIdRequest(-1));
                this.newTxRequests.AddNewResource(new NewTxIdRequest(-1));
                this.updateCommitLowerBoundRequests.AddNewResource(new UpdateCommitLowerBoundRequest(-1, 0));
                this.setCommitTsRequests.AddNewResource(new SetCommitTsRequest(-1, 0));
                this.recycleTxRequests.AddNewResource(new RecycleTxRequest(-1));
                this.updateTxStatusRequests.AddNewResource(new UpdateTxStatusRequest(-1, TxStatus.Aborted));
                this.removeTxRequests.AddNewResource(new RemoveTxRequest(-1));

                this.readSetEntries.AddNewResource(new ReadSetEntry());
                this.postprocessingEntries.AddNewResource(new PostProcessingEntry());
                this.writeSetEntries.AddNewResource(new WriteSetEntry());
                this.versionKeyEntries.AddNewResource(new VersionKeyEntry());

                this.txTableEntries.Enqueue(new TxTableEntry());
                this.versionEntries.Enqueue(new VersionEntry());
                this.transRequests.AddNewResource(new TransactionRequest());
            }
        }

        // Free the tx Request
        internal void RecycleTxRequest(ref TxRequest req)
        {
            req.Free();
            req = null;
        }

        internal void RecycleTxSetEntry(ref TxSetEntry entry)
        {
            entry.Free();
            entry = null;
        }

        internal ReadSetEntry GetReadSetEntry(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTimestamp,
            long endTimestamp,
            long txId,
            object record,
            long tailKey)
        {
            ReadSetEntry entry = this.readSetEntries.GetResource();
            if (entry == null)
            {
                entry = new ReadSetEntry();
                entry.Use();
                this.readSetEntries.AddNewResource(entry);
            }

            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.VersionKey = versionKey;
            entry.BeginTimestamp = beginTimestamp;
            entry.EndTimestamp = endTimestamp;
            entry.TxId = txId;
            entry.Record = record;
            entry.TailKey = tailKey;

            return entry;
        }

        internal PostProcessingEntry GetPostProcessingEntry(
            string tableId, 
            object recordKey, 
            long versionKey, 
            long beginTimestamp, 
            long endTimestamp)
        {
            PostProcessingEntry entry = this.postprocessingEntries.GetResource();
            if (entry == null)
            {
                entry = new PostProcessingEntry();
                entry.Use();
                this.postprocessingEntries.AddNewResource(entry);
            }

            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.VersionKey = versionKey;
            entry.BeginTimestamp = beginTimestamp;
            entry.EndTimestamp = endTimestamp;

            return entry;
        }

        internal WriteSetEntry GetWriteSetEntry(string tableId, object recordKey, object payload, long versionKey)
        {
            WriteSetEntry entry = this.writeSetEntries.GetResource();
            if (entry == null)
            {
                entry = new WriteSetEntry();
                entry.Use();
                this.writeSetEntries.AddNewResource(entry);
            }

            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.Payload = payload;
            entry.VersionKey = versionKey;
            return entry;
        }

        internal VersionKeyEntry GetVersionKeyEntry(string tableId, object recordKey, long versionKey)
        {
            VersionKeyEntry entry = this.versionKeyEntries.GetResource();
            if (entry == null)
            {
                entry = new VersionKeyEntry();
                entry.Use();
                this.versionKeyEntries.AddNewResource(entry);
            }
            entry.TableId = tableId;
            entry.RecordKey = recordKey;
            entry.VersionKey = versionKey;

            return entry;
        }

        internal TxTableEntry GetTxTableEntry()
        {
            if (this.txTableEntries.Count > 0)
            {
                return this.txTableEntries.Dequeue();
            }
            else
            {
                return new TxTableEntry();
            }
        }

        internal void RecycleTxTableEntry(ref TxTableEntry entry)
        {
            this.txTableEntries.Enqueue(entry);
            entry = null;
        }

        internal VersionEntry GetVersionEntry()
        {
            if (this.versionEntries.Count > 0)
            {
                return this.versionEntries.Dequeue();
            }
            else
            {
                return new VersionEntry();
            }
        }

        internal void RecycleVersionEntry(ref VersionEntry entry)
        {
            this.versionEntries.Enqueue(entry);
            entry = null;
        }

        /// <summary>
        /// VersionEntry related recycling
        /// </summary>
        internal GetVersionListRequest GetVersionListRequest(string tableId, object recordKey)
        {
            GetVersionListRequest req = this.getVersionListRequests.GetResource();
            if (req == null)
            {
                req = new GetVersionListRequest(tableId, recordKey, null);
                req.Use();
                this.getVersionListRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
            }
            
            return req;
        }

        internal void RecycleGetVersionListRequest(ref GetVersionListRequest req)
        {
            req.Container = null;
            this.getVersionListRequests.Recycle(req);
            req = null;
        }

        internal InitiGetVersionListRequest GetInitiGetVersionListRequest(string tableId, object recordKey)
        {
            InitiGetVersionListRequest req = this.initiGetVersionListRequests.GetResource();

            if (req == null)
            {
                req = new InitiGetVersionListRequest(tableId, recordKey, null);
                req.Use();
                this.initiGetVersionListRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
            }

            return req;
        }

        internal void RecycleInitiGetVersionListRequest(ref InitiGetVersionListRequest req)
        {
            req.Container = null;
            this.initiGetVersionListRequests.Recycle(req);
            req = null;
        }

        internal ReadVersionRequest ReadVersionRequest(string tableId, object recordKey, long versionKey)
        {
            ReadVersionRequest req = this.readVersionRequests.GetResource();
            if (req == null)
            {
                req = new ReadVersionRequest(tableId, recordKey, versionKey);
                req.Use();
                this.readVersionRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
                req.VersionKey = versionKey;
            }
            return req;
        }

        internal void RecycleReadVersionRequest(ref ReadVersionRequest req)
        {
            this.readVersionRequests.Recycle(req);
            req = null;
        }

        internal ReplaceVersionRequest ReplaceVersionRequest(
            string tableId,
            object recordKey,
            long versionKey,
            long beginTime,
            long endTime,
            long txId,
            long readTxId,
            long expectedEndTimestamp)
        {
            ReplaceVersionRequest req = this.replaceVersionRequests.GetResource();
            if (req == null)
            {
                req = new ReplaceVersionRequest(tableId, recordKey, versionKey, beginTime, endTime,
                    txId, readTxId, expectedEndTimestamp);
                req.Use();
                this.replaceVersionRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
                req.VersionKey = versionKey;
                req.BeginTs = beginTime;
                req.EndTs = endTime;
                req.TxId = txId;
                req.ReadTxId = readTxId;
                req.ExpectedEndTs = expectedEndTimestamp;
            }
            return req;
        }

        internal void RecycleReplaceVersionRequest(ref ReplaceVersionRequest req)
        {
            this.replaceVersionRequests.Recycle(req);
            req = null;
        }

        internal ReplaceWholeVersionRequest ReplaceWholeVersionRequest(
            string tableId, 
            object recordKey, 
            long versionKey, 
            VersionEntry entry)
        {
            ReplaceWholeVersionRequest req = this.replaceWholeVersionRequests.GetResource();
            if (req == null)
            {
                req = new ReplaceWholeVersionRequest(tableId, recordKey, versionKey, entry);
                req.Use();
                this.replaceWholeVersionRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
                req.VersionKey = versionKey;
                req.VersionEntry = entry;
            }
            return req;
        }

        internal void RecycleReplaceWholeVersionRequest(ref ReplaceWholeVersionRequest req)
        {
            this.replaceWholeVersionRequests.Recycle(req);
            req = null;
        }

        internal DeleteVersionRequest DeleteVersionRequest(string tableId, object recordKey, long versionKey)
        {
            DeleteVersionRequest req = this.deleteVersionRequests.GetResource();
            if (req == null)
            {
                req = new DeleteVersionRequest(tableId, recordKey, versionKey);
                req.Use();
                this.deleteVersionRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
                req.VersionKey = versionKey;
            }
            return req;
        }

        internal void RecycleDeleteVersionRequest(ref DeleteVersionRequest req)
        {
            this.deleteVersionRequests.Recycle(req);
            req = null;
        }

        internal UpdateVersionMaxCommitTsRequest UpdateVersionMaxCommitTsRequest(
            string tableId,
            object recordKey,
            long versionKey,
            long maxCommitTs)
        {
            UpdateVersionMaxCommitTsRequest req = this.updateVersionMaxCommitTsRequests.GetResource();
            if (req == null)
            {
                req = new UpdateVersionMaxCommitTsRequest(tableId, recordKey, versionKey, maxCommitTs);
                req.Use();
                this.updateVersionMaxCommitTsRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
                req.VersionKey = versionKey;
                req.MaxCommitTs = maxCommitTs;
            }
            return req;
        }

        internal void RecycleUpdateVersiomMaxCommitTsRequest(ref UpdateVersionMaxCommitTsRequest req)
        {
            this.updateVersionMaxCommitTsRequests.Recycle(req);
            req = null;
        }

        internal UploadVersionRequest UploadVersionRequest(
            string tableId, 
            object recordKey, 
            long versionKey,
            VersionEntry entry)
        {
            UploadVersionRequest req = this.uploadVersionRequests.GetResource();
            if (req == null)
            {
                req = new UploadVersionRequest(tableId, recordKey, versionKey, entry);
                req.Use();
                this.uploadVersionRequests.AddNewResource(req);
            }
            else
            {
                req.TableId = tableId;
                req.RecordKey = recordKey;
                req.VersionKey = versionKey;
                req.VersionEntry = entry;
            }
            return req;
        }

        internal void RecycleUploadVersionEntry(ref UploadVersionRequest req)
        {
            this.uploadVersionRequests.Recycle(req);
            req = null;
        }

        /// <summary>
        /// TxEntryRequest related recycling
        /// </summary>
        internal GetTxEntryRequest GetTxEntryRequest(long txId)
        {
            GetTxEntryRequest req = this.getTxEntryRequests.GetResource();
            if (req == null)
            {
                req = new GetTxEntryRequest(txId);
                req.Use();
                this.getTxEntryRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
            }
            
            return req;
        }

        internal void RecycleGetTxEntryRequest(ref GetTxEntryRequest req)
        {
            this.getTxEntryRequests.Recycle(req);
            req = null;
        }

        internal InsertTxIdRequest InsertTxRequest(long txId)
        {
            InsertTxIdRequest req = this.inserTxRequests.GetResource();
            if (req == null)
            {
                req = new InsertTxIdRequest(txId);
                req.Use();
                this.inserTxRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
            }
            
            return req;
        }

        internal void RecycleInsertTxRequest(ref InsertTxIdRequest req)
        {
            this.inserTxRequests.Recycle(req);
            req = null;
        }
        
        internal NewTxIdRequest NewTxIdRequest(long txId)
        {
            NewTxIdRequest req = this.newTxRequests.GetResource();
            if (req == null)
            {
                req = new NewTxIdRequest(txId);
                req.Use();
                this.newTxRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
            }
            
            return req;
        }

        internal void RecycleNewTxIdRequest(ref NewTxIdRequest req)
        {
            this.newTxRequests.Recycle(req);
            req = null;
        }

        internal UpdateCommitLowerBoundRequest UpdateCommitLowerBound(long txId, long lowerBound)
        {
            UpdateCommitLowerBoundRequest req = this.updateCommitLowerBoundRequests.GetResource();
            if (req == null)
            {
                req = new UpdateCommitLowerBoundRequest(txId, lowerBound);
                req.Use();
                this.updateCommitLowerBoundRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
                req.CommitTsLowerBound = lowerBound;
            }

            return req;
        }

        internal void RecycleUpateCommitLowerBound(ref UpdateCommitLowerBoundRequest req)
        {
            this.updateCommitLowerBoundRequests.Recycle(req);
            req = null;
        }

        internal SetCommitTsRequest SetCommitTsRequest(long txId, long commitTs)
        {
            SetCommitTsRequest req = this.setCommitTsRequests.GetResource();
            if (req == null)
            {
                req = new SetCommitTsRequest(txId, commitTs);
                req.Use();
                this.setCommitTsRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
                req.ProposedCommitTs = commitTs;
            }

            return req;
        }

        internal void RecycleSetCommitTsRequest(ref SetCommitTsRequest req)
        {
            this.setCommitTsRequests.Recycle(req);
            req = null;
        }

        internal RecycleTxRequest RecycleTxRequest(long txId)
        {
            RecycleTxRequest req = this.recycleTxRequests.GetResource();
            if (req == null)
            {
                req = new RecycleTxRequest(txId);
                req.Use();
                this.recycleTxRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
            }

            return req;
        }

        internal void RecycleRecycleTxRequest(ref RecycleTxRequest req)
        {
            this.recycleTxRequests.Recycle(req);
            req = null;
        }

        internal UpdateTxStatusRequest UpdateTxStatusRequest(long txId, TxStatus status)
        {
            UpdateTxStatusRequest req = this.updateTxStatusRequests.GetResource();
            if (req == null)
            {
                req = new UpdateTxStatusRequest(txId, status);
                req.Use();
                this.updateTxStatusRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
                req.TxStatus = status;
            }
            return req;
        }

        internal void RecycleUpdateTxStatusRequest(ref UpdateTxStatusRequest req)
        {
            this.updateTxStatusRequests.Recycle(req);
            req = null;
        }

        internal RemoveTxRequest RemoveTxRequest(long txId)
        {
            RemoveTxRequest req = this.removeTxRequests.GetResource();
            if (req == null)
            {
                req = new RemoveTxRequest(txId);
                req.Use();
                this.removeTxRequests.AddNewResource(req);
            }
            else
            {
                req.TxId = txId;
            }
            return req;
        }
        
        internal void RecycleRemoveTxRequest(ref RemoveTxRequest req)
        {
            this.removeTxRequests.Recycle(req);
            req = null;
        }

        internal TransactionRequest TransactionRequest(
            string sessionId,
            string tableId,
            object key,
            object value,
            OperationType operationType)
        {
            TransactionRequest transReq = this.transRequests.GetResource();
            if (transReq == null)
            {
                transReq = new TransactionRequest();
                transReq.Use();
                this.transRequests.AddNewResource(transReq);
            }

            transReq.SessionId = sessionId;
            transReq.TableId = tableId;
            transReq.RecordKey = key;
            transReq.Payload = value;
            transReq.OperationType = operationType;

            return transReq;
        }

        internal void RecycleTransRequest(ref TransactionRequest transReq)
        {
            transReq.Free();
            transReq = null;
        }
    }
}
