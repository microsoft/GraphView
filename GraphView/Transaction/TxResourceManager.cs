
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;

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

        // The list will be shared by get version entries and validate version entries
        private readonly Queue<List<VersionEntry>> versionLists;
        private readonly Queue<List<string>> tableIdList;
        private readonly Queue<List<object>> recordKeyList;

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
        private readonly Queue<ReadSetEntry> readSetEntries;
        private readonly Queue<PostProcessingEntry> postprocessingEntries;

        public TxResourceManager()
        {
            // The list will be shared by get version entries and validate version entries
            // Thus the size should be double
            this.versionLists = new Queue<List<VersionEntry>>(2 * TxResourceManager.workingsetCapacity);
            // The list will be shared by writeKeyList and validateKeyList
            this.tableIdList = new Queue<List<string>>(2 * TxResourceManager.workingsetCapacity);
            this.recordKeyList = new Queue<List<object>>(TxResourceManager.workingsetCapacity);

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

            this.readSetEntries = new Queue<ReadSetEntry>();
            this.postprocessingEntries = new Queue<PostProcessingEntry>();

            for (int i = 0; i < TxResourceManager.workingsetCapacity; i++)
            {
                this.versionLists.Enqueue(new List<VersionEntry>(8));
                this.tableIdList.Enqueue(new List<string>(8));
                this.recordKeyList.Enqueue(new List<object>(8));

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

                this.readSetEntries.Enqueue(new ReadSetEntry());
                this.postprocessingEntries.Enqueue(new PostProcessingEntry());
            }
        }

        // Free the tx Request
        internal void RecycleTxRequest(ref TxRequest req)
        {
            req.Free();
            req = null;
        }

        internal List<VersionEntry> GetVersionList()
        {
            if (this.versionLists.Count > 0)
            {
                return this.versionLists.Dequeue();
            }
            else
            {
                List<VersionEntry> list = new List<VersionEntry>(8);
                // Duplicated Enqueue
                // this.versionLists.Enqueue(list);
                return list;
            }
        }

        internal void RecycleVersionList(ref List<VersionEntry> list)
        {
            list.Clear();
            this.versionLists.Enqueue(list);
            list = null;
        }

        internal List<String> GetTableIdList()
        {
            if (this.tableIdList.Count > 0)
            {
                return this.tableIdList.Dequeue();
            }
            else
            {
                List<string> tableIdList = new List<string>(8);
                return tableIdList;
            }
        }

        internal void RecycleTableIdList(ref List<string> list)
        {
            list.Clear();
            this.tableIdList.Enqueue(list);
            list = null;
        }

        internal List<object> GetRecordKeyLisy()
        {
            if (this.recordKeyList.Count > 0)
            {
                return this.recordKeyList.Dequeue();
            }
            else
            {
                List<object> list = new List<object>(8);
                return list;
            }
        }

        internal void RecycleRecordKeyList(ref List<object> list)
        {
            list.Clear();
            this.recordKeyList.Enqueue(list);
            list = null;
        }

        internal ReadSetEntry GetReadSetEntry()
        {
            if (this.readSetEntries.Count > 0)
            {
                return this.readSetEntries.Dequeue();
            }
            else
            {
                return new ReadSetEntry();
            }
        }

        internal void RecycleReadSetEntry(ref ReadSetEntry entry)
        {
            this.readSetEntries.Enqueue(entry);
            entry = null;
        }

        internal PostProcessingEntry GetPostProcessingEntry()
        {
            if (this.postprocessingEntries.Count > 0)
            {
                return this.postprocessingEntries.Dequeue();
            }
            else
            {
                return new PostProcessingEntry();
            }
        }

        internal void RecyclePostProcessingEntry(ref PostProcessingEntry entry)
        {
            this.postprocessingEntries.Enqueue(entry);
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
    }
}
