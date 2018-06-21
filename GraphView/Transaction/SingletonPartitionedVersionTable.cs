
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
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
        /// Spinlocks for partitions
        /// </summary>

        private readonly PartitionedVersionTableVisitor[] requestVisitors;

        private static readonly int RECORD_CAPACITY = 1000000;

        internal static readonly int VERSION_CAPACITY = 16;

        internal int PartitionCount { get; private set; }

        public SingletonPartitionedVersionTable(VersionDb versionDb, string tableId, int partitionCount)
            : base(versionDb, tableId, partitionCount)
        {
            this.PartitionCount = partitionCount;
            this.dicts = new Dictionary<object, Dictionary<long, VersionEntry>>[partitionCount];

            for (int pid = 0; pid < partitionCount; pid++)
            {
                this.dicts[pid] = new Dictionary<object, Dictionary<long, VersionEntry>>(SingletonPartitionedVersionTable.RECORD_CAPACITY);
                this.tableVisitors[pid] = new PartitionedVersionTableVisitor(this.dicts[pid]);
            }
        }

        internal override void EnqueueVersionEntryRequest(VersionEntryRequest req, int execPartition = 0)
        {
            // Interlocked.Increment(ref SingletonPartitionedVersionDb.EnqueuedRequests);

            // SingletonPartitionedVersionDb implementation 1
            base.EnqueueVersionEntryRequest(req, execPartition);
            while (!req.Finished) ;

            // SingletonPartitionedVersionDb implementation 2
            //int pk = this.VersionDb.PhysicalPartitionByKey(req.RecordKey);
            //if (pk == execPartition)
            //{
            //    this.tableVisitors[pk].Invoke(req);
            //}
            //else
            //{
            //    base.EnqueueVersionEntryRequest(req, execPartition);
            //}
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
    }
}
