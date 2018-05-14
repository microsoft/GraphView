
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    internal class TxResourceManager
    {
        internal static readonly int workingsetCapacity = 100;

        private readonly Queue<List<VersionEntry>> versionLists;

        // Version entry requests
        private readonly Queue<GetVersionListRequest> getVersionListRequests;
        private readonly Queue<InitiGetVersionListRequest> initiGetVersionListRequests;

        // Tx entry requests
        private readonly Queue<GetTxEntryRequest> getTxEntryRequests;

        public TxResourceManager()
        {
            this.versionLists = new Queue<List<VersionEntry>>(TxResourceManager.workingsetCapacity);

            this.getVersionListRequests = new Queue<GetVersionListRequest>(TxResourceManager.workingsetCapacity);
            this.initiGetVersionListRequests = new Queue<InitiGetVersionListRequest>(TxResourceManager.workingsetCapacity);

            this.getTxEntryRequests = new Queue<GetTxEntryRequest>(TxResourceManager.workingsetCapacity);

            for (int i = 0; i < TxResourceManager.workingsetCapacity; i++)
            {
                this.versionLists.Enqueue(new List<VersionEntry>(8));

                this.getVersionListRequests.Enqueue(new GetVersionListRequest(null, null, null));
                this.initiGetVersionListRequests.Enqueue(new InitiGetVersionListRequest(null, null, null));

                this.getTxEntryRequests.Enqueue(new GetTxEntryRequest(-1));
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

        internal void RecycleVersionList(List<VersionEntry> list)
        {
            list.Clear();
            this.versionLists.Enqueue(list);
        }

        internal GetVersionListRequest GetVersionListRequest(string tableId, object recordKey)
        {
            int count = 0;
            int size = this.getVersionListRequests.Count;

            GetVersionListRequest req = this.getVersionListRequests.Dequeue();
            while (req.InUse)
            {
                this.getVersionListRequests.Enqueue(req);
                count++;

                if (count >= size)
                {
                    req = new GetVersionListRequest(null, null, null);
                    this.getVersionListRequests.Enqueue(req);
                    break;
                }
                else
                {
                    req = this.getVersionListRequests.Dequeue();
                }
            }

            req.TableId = tableId;
            req.RecordKey = recordKey;
            return req;
        }

        internal void RecycleGetVersionListRequest(GetVersionListRequest req)
        {
            req.InUse = false;
            req.Container = null;
            this.getVersionListRequests.Enqueue(req);
        }

        internal InitiGetVersionListRequest GetInitiGetVersionListRequest(string tableId, object recordKey)
        {
            int count = 0;
            int size = this.initiGetVersionListRequests.Count;

            InitiGetVersionListRequest req = this.initiGetVersionListRequests.Dequeue();
            while (req.InUse)
            {
                this.initiGetVersionListRequests.Enqueue(req);
                count++;

                if (count >= size)
                {
                    req = new InitiGetVersionListRequest(null, null, null);
                    this.initiGetVersionListRequests.Enqueue(req);
                    break;
                }
                else
                {
                    req = this.initiGetVersionListRequests.Dequeue();
                }
            }

            req.TableId = tableId;
            req.RecordKey = recordKey;
            return req;
        }

        internal void RecycleInitiGetVersionListRequest(InitiGetVersionListRequest req)
        {
            req.InUse = false;
            req.Container = null;
            this.initiGetVersionListRequests.Enqueue(req);
        }

        internal GetTxEntryRequest GetTxEntryRequest(long txId)
        {
            int count = 0;
            int size = this.getTxEntryRequests.Count;

           GetTxEntryRequest req = this.getTxEntryRequests.Dequeue();
            while (req.InUse)
            {
                this.getTxEntryRequests.Enqueue(req);
                count++;

                if (count >= size)
                {
                    req = new GetTxEntryRequest(txId);
                    this.getTxEntryRequests.Enqueue(req);
                    break;
                }
                else
                {
                    req = this.getTxEntryRequests.Dequeue();
                }
            }

            req.TxId = txId;
            return req;
        }

        internal void RecycleGetTxEntryRequest(GetTxEntryRequest req)
        {
            req.InUse = false;
            req.TxId = -1;
            this.getTxEntryRequests.Enqueue(req);
        }
    }
}
