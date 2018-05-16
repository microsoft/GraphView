
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;

    internal class SingletonPartitionedVersionTable : VersionTable
    {
        /// <summary>
        /// A dict array to store all versions, recordKey => {versionKey => versionEntry}
        /// Every version table may be stored on several partitions, and for every partition, it has a dict
        /// 
        /// The idea to use the version entry rather than versionBlob is making sure never create a new version entry
        /// unless upload it
        /// </summary>
        private readonly Dictionary<object, Dictionary<long, VersionEntry>>[] dicts;

        /// <summary>
        /// Request queues for partitions
        /// </summary>
        private readonly Queue<VersionEntryRequest>[] requestQueues;

        /// <summary>
        /// A queue of version entry requests for each partition to be flushed to the k-v store
        /// </summary>
        private readonly Queue<VersionEntryRequest>[] flushQueues;

        /// <summary>
        /// Spinlocks for partitions
        /// </summary>
        private readonly SpinLock[] queueLocks;

        private readonly PartitionVersionEntryRequestVisitor[] requestVisitors;

        private static readonly int RECORD_CAPACITY = 1000000;

        internal static readonly int VERSION_CAPACITY = 16;

        internal int PartitionCount { get; private set; }

        public SingletonPartitionedVersionTable(VersionDb versionDb, string tableId, int partitionCount)
            : base (versionDb, tableId)
        {
            this.PartitionCount = partitionCount;
            this.dicts = new Dictionary<object, Dictionary<long, VersionEntry>>[partitionCount];
            this.requestQueues = new Queue<VersionEntryRequest>[partitionCount];
            this.flushQueues = new Queue<VersionEntryRequest>[partitionCount];
            this.queueLocks = new SpinLock[partitionCount];
            this.requestVisitors = new PartitionVersionEntryRequestVisitor[partitionCount];

            for (int pid = 0; pid < partitionCount; pid ++)
            {
                this.dicts[pid] = new Dictionary<object, Dictionary<long, VersionEntry>>(SingletonPartitionedVersionTable.RECORD_CAPACITY);
                this.requestQueues[pid] = new Queue<VersionEntryRequest>();
                this.flushQueues[pid] = new Queue<VersionEntryRequest>();
                this.queueLocks[pid] = new SpinLock();
                this.requestVisitors[pid] = new PartitionVersionEntryRequestVisitor(this.dicts[pid]);
            }
        }

        internal override void Clear()
        {
            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                this.dicts[pid].Clear();
                this.requestQueues[pid].Clear();
                this.flushQueues[pid].Clear();
            }
        }

        internal override void EnqueueVersionEntryRequest(VersionEntryRequest req)
        {
            SingletonPartitionedVersionDb.CommandsEnqueued += 1;
            int pk = this.VersionDb.PhysicalPartitionByKey(req.RecordKey);

            bool lockTaken = false;
            try
            {
                this.queueLocks[pk].Enter(ref lockTaken);
                this.requestQueues[pk].Enqueue(req);
            }
            finally
            {
                if (lockTaken)
                {
                    this.queueLocks[pk].Exit();
                }
            }
        }

        /// <summary>
        /// Move pending requests of a version table partition to the partition's flush queue. 
        /// </summary>
        /// <param name="pk">The key of the version table partition to flush</param>
        private void DequeueRequests(int pk)
        {
            // Check whether the queue is empty at first
            if (this.requestQueues[pk].Count > 0)
            {
                bool lockTaken = false;
                try
                {
                    this.queueLocks[pk].Enter(ref lockTaken);
                    // In case other running threads also flush the same queue
                    if (this.requestQueues[pk].Count > 0)
                    {
                        Queue<VersionEntryRequest> freeQueue = this.flushQueues[pk];
                        this.flushQueues[pk] = this.requestQueues[pk];
                        this.requestQueues[pk] = freeQueue;
                        SingletonPartitionedVersionDb.CommandsFromQueue += this.flushQueues[pk].Count;
                    }
                }
                finally
                {
                    if (lockTaken)
                    {
                        this.queueLocks[pk].Exit();
                    }
                }
            }
        }

        private int localIndex = 0;

        internal override void Visit(int partitionKey)
        {
            localIndex = 0;
            this.DequeueRequests(partitionKey);
            Queue<VersionEntryRequest> flushQueue = this.flushQueues[partitionKey];

            if (flushQueue.Count == 0)
            {
                return;
            }

            SingletonPartitionedVersionDb.CommandsFlushed += flushQueue.Count;
            PartitionVersionEntryRequestVisitor visitor = this.requestVisitors[partitionKey];
            foreach (VersionEntryRequest req in flushQueue)
            {
                localIndex++;
                Console.WriteLine("Name:{0}, RecordKey:{1}", req.GetType().Name, req.RecordKey);
                visitor.Invoke(req);
            }
            flushQueue.Clear();
        }
    }
}
