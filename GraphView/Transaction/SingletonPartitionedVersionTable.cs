

namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class SingletonPartitionedVersionTable : VersionTable
    {
        private readonly Dictionary<object, Dictionary<long, VersionBlob>>[] dicts;
        private readonly Queue<VersionEntryRequest>[] requestQueues;
        private readonly SpinLock[] queueLocks;

        private static readonly long TAIL_KEY = -1L;

        private static readonly int RECORD_CAPACITY = 1000000;

        private static readonly int VERSION_CAPACITY = 32;

        public SingletonPartitionedVersionTable(VersionDb versionDb, string tableId, int partitionCount)
            : base (versionDb, tableId)
        {
            this.dicts = new Dictionary<object, Dictionary<long, VersionBlob>>[partitionCount];
            this.requestQueues = new Queue<VersionEntryRequest>[partitionCount];
            this.queueLocks = new SpinLock[partitionCount];

            for (int pid = 0; pid < partitionCount; pid ++)
            {
                this.dicts[pid] = new Dictionary<object, Dictionary<long, VersionBlob>>(SingletonPartitionedVersionTable.RECORD_CAPACITY);
                this.requestQueues[pid] = new Queue<VersionEntryRequest>();
                this.queueLocks[pid] = new SpinLock();
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

        private VersionEntryRequest DequeueRequest(int pk)
        {
            VersionEntryRequest req = null;
            Queue<VersionEntryRequest> queue = this.requestQueues[pk];

            bool lockTaken = false;
            try
            {
                this.queueLocks[pk].Enter(ref lockTaken);
                if (queue.Count > 0)
                {
                    req = queue.Dequeue();
                }
            }
            finally
            {
                if (lockTaken)
                {
                    this.queueLocks[pk].Exit();
                }
            }

            return req;
        }

        internal override void Visit(int partitionKey)
        {
            base.Visit(partitionKey);
        }
    }
}
