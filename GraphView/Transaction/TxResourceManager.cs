
namespace GraphView.Transaction
{
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
            int count = 0;
            int size = this.resourcePool.Count;

            T resource = this.resourcePool.Dequeue();
            while (resource.IsActive())
            {
                this.resourcePool.Enqueue(resource);
                count++;

                if (count >= size)
                {
                    return default(T);
                }
                else
                {
                    resource = this.resourcePool.Dequeue();
                }
            }

            resource.Use();
            return resource;
        }

        internal void Recycle(T resource)
        {
            resource.Free();
            this.resourcePool.Enqueue(resource);
        }

        internal void AddNewResource(T resource)
        {
            resource.Free();
            this.resourcePool.Enqueue(resource);
        } 
    }

    internal class TxResourceManager
    {
        internal static readonly int workingsetCapacity = 100;

        private readonly Queue<List<VersionEntry>> versionLists;

        // Version entry requests
        private readonly ResourcePool<GetVersionListRequest> getVersionListRequests;
        private readonly ResourcePool<InitiGetVersionListRequest> initiGetVersionListRequests;

        // Tx entry requests
        private readonly ResourcePool<GetTxEntryRequest> getTxEntryRequests;
        private readonly ResourcePool<InsertTxIdRequest> inserTxRequests;
        private readonly ResourcePool<NewTxIdRequest> newTxRequests;
        private readonly ResourcePool<UpdateCommitLowerBoundRequest> updateCommitLowerBoundRequests;
        private readonly ResourcePool<SetCommitTsRequest> setCommitTsRequests;
        private readonly ResourcePool<RecycleTxRequest> recycleTxRequests;

        public TxResourceManager()
        {
            this.versionLists = new Queue<List<VersionEntry>>(TxResourceManager.workingsetCapacity);

            this.getVersionListRequests = new ResourcePool<GetVersionListRequest>(TxResourceManager.workingsetCapacity);
            this.initiGetVersionListRequests = new ResourcePool<InitiGetVersionListRequest>(TxResourceManager.workingsetCapacity);

            this.getTxEntryRequests = new ResourcePool<GetTxEntryRequest>(TxResourceManager.workingsetCapacity);
            this.inserTxRequests = new ResourcePool<InsertTxIdRequest>(TxResourceManager.workingsetCapacity);
            this.newTxRequests = new ResourcePool<NewTxIdRequest>(TxResourceManager.workingsetCapacity);
            this.updateCommitLowerBoundRequests = 
                new ResourcePool<UpdateCommitLowerBoundRequest>(TxResourceManager.workingsetCapacity);
            this.setCommitTsRequests = new ResourcePool<SetCommitTsRequest>(TxResourceManager.workingsetCapacity);
            this.recycleTxRequests = new ResourcePool<RecycleTxRequest>(TxResourceManager.workingsetCapacity);

            for (int i = 0; i < TxResourceManager.workingsetCapacity; i++)
            {
                this.versionLists.Enqueue(new List<VersionEntry>(8));

                this.getVersionListRequests.AddNewResource(new GetVersionListRequest(null, null, null));
                this.initiGetVersionListRequests.AddNewResource(new InitiGetVersionListRequest(null, null, null));

                this.getTxEntryRequests.AddNewResource(new GetTxEntryRequest(-1));
                this.inserTxRequests.AddNewResource(new InsertTxIdRequest(-1));
                this.newTxRequests.AddNewResource(new NewTxIdRequest(-1));
                this.updateCommitLowerBoundRequests.AddNewResource(new UpdateCommitLowerBoundRequest(-1, 0));
                this.setCommitTsRequests.AddNewResource(new SetCommitTsRequest(-1, 0));
                this.recycleTxRequests.AddNewResource(new RecycleTxRequest(-1));
            }
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
                this.versionLists.Enqueue(list);
                return list;
            }
        }

        internal void RecycleVersionList(ref List<VersionEntry> list)
        {
            list.Clear();
            this.versionLists.Enqueue(list);
            list = null;
        }

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
    }
}
