
namespace GraphView.Transaction
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;

    internal class SingletonPartitionedVersionTable : VersionTable
    {
        private readonly Dictionary<object, Dictionary<long, VersionEntry>>[] dicts;
        private readonly Queue<VersionEntryRequest>[] requestQueues;
        private readonly SpinLock[] queueLocks;
        private readonly PartitionVersionEntryRequestVisitor[] requestVisitors;

        private static readonly int RECORD_CAPACITY = 1000000;

        internal static readonly int VERSION_CAPACITY = 16;

        internal static readonly long TAIL_KEY = -1L;

        public SingletonPartitionedVersionTable(VersionDb versionDb, string tableId, int partitionCount)
            : base (versionDb, tableId)
        {
            this.dicts = new Dictionary<object, Dictionary<long, VersionEntry>>[partitionCount];
            this.requestQueues = new Queue<VersionEntryRequest>[partitionCount];
            this.queueLocks = new SpinLock[partitionCount];
            this.requestVisitors = new PartitionVersionEntryRequestVisitor[partitionCount];

            for (int pid = 0; pid < partitionCount; pid ++)
            {
                this.dicts[pid] = new Dictionary<object, Dictionary<long, VersionEntry>>(SingletonPartitionedVersionTable.RECORD_CAPACITY);
                this.requestQueues[pid] = new Queue<VersionEntryRequest>();
                this.queueLocks[pid] = new SpinLock();
                this.requestVisitors[pid] = new PartitionVersionEntryRequestVisitor(this.dicts[pid]);
            }
        }

        internal override void EnqueueTxRequest(TxRequest req)
        {
            Debug.Assert(req is VersionEntryRequest);

            VersionEntryRequest verReq = req as VersionEntryRequest;
            int pk = this.VersionDb.PhysicalPartitionByKey(verReq.RecordKey);

            bool lockTaken = false;
            try
            {
                this.queueLocks[pk].Enter(ref lockTaken);
                this.requestQueues[pk].Enqueue(verReq);
            }
            finally
            {
                if (lockTaken)
                {
                    this.queueLocks[pk].Exit();
                }
            }
        }

        private IEnumerable<VersionEntryRequest> DequeueRequests(int pk)
        {
            Queue<VersionEntryRequest> queue = this.requestQueues[pk];
            List<VersionEntryRequest> reqList = null;  

            bool lockTaken = false;
            try
            {
                this.queueLocks[pk].Enter(ref lockTaken);
                if (queue.Count > 0)
                {
                    reqList = new List<VersionEntryRequest>(queue);
                    queue.Clear();
                }
            }
            finally
            {
                if (lockTaken)
                {
                    this.queueLocks[pk].Exit();
                }
            }

            return reqList;
        }

        internal override void Visit(int partitionKey)
        {
            IEnumerable<VersionEntryRequest> toDoList = this.DequeueRequests(partitionKey);

            if (toDoList == null)
            {
                return;
            }

            PartitionVersionEntryRequestVisitor visitor = this.requestVisitors[partitionKey];
            foreach (VersionEntryRequest req in toDoList)
            {
                visitor.Invoke(req);
            }
        }
    }
}
