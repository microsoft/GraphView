
namespace GraphView.Transaction
{
    using System.Collections.Generic;

    internal class TxResourceManager
    {
        internal static readonly int workingsetCapacity = 100;

        private readonly Queue<List<VersionEntry>> versionLists;

        private readonly Queue<GetVersionListRequest> getVersionListRequests;
        private readonly Queue<InitiGetVersionListRequest> initiGetVersionListRequests;

        public TxResourceManager()
        {
            this.versionLists = new Queue<List<VersionEntry>>(TxResourceManager.workingsetCapacity);
            this.getVersionListRequests = new Queue<GetVersionListRequest>(TxResourceManager.workingsetCapacity);
            this.initiGetVersionListRequests = new Queue<InitiGetVersionListRequest>(TxResourceManager.workingsetCapacity);

            for (int i = 0; i < TxResourceManager.workingsetCapacity; i++)
            {
                this.versionLists.Enqueue(new List<VersionEntry>(8));
                this.getVersionListRequests.Enqueue(new GetVersionListRequest(null, null, null));
                this.initiGetVersionListRequests.Enqueue(new InitiGetVersionListRequest(null, null, null));
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

        internal GetVersionListRequest GetVersionListRequest()
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

            return req;
        }

        internal void RecycleGetVersionListRequest(GetVersionListRequest req)
        {
            req.InUse = false;
            req.Container = null;
            this.getVersionListRequests.Enqueue(req);
        }

        internal InitiGetVersionListRequest GetInitiGetVersionListRequest()
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

            return req;
        }

        internal void RecycleInitiGetVersionListRequest(InitiGetVersionListRequest req)
        {
            req.InUse = false;
            req.Container = null;
            this.initiGetVersionListRequests.Enqueue(req);
        }
    }
}
