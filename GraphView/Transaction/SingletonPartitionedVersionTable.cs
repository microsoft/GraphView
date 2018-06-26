
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
        private Dictionary<object, Dictionary<long, VersionEntry>>[] dicts;

        /// <summary>
        /// Spinlocks for partitions
        /// </summary>
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
                this.tableVisitors[pid] = new SingletonPartitionedVersionTableVisitor(this.dicts[pid]);
            }
        }

        /// <summary>
        /// Only for benchmark test
        /// 
        /// Keep the number of records as the same and add new partitions to the given expectedPartitionCount
        /// That means add new visitors and dicts, finally reshuffle all data into the right partitions
        /// </summary>
        /// <param name="part"></param>
        public void ExtendPartition(int expectedPartitionCount)
        {
            int prePartitionCount = this.PartitionCount;
            this.PartitionCount = expectedPartitionCount;

            // Resize Visitors and Dicts
            Array.Resize(ref this.dicts, expectedPartitionCount);
            for (int pk = prePartitionCount; pk < expectedPartitionCount; pk++)
            {
                this.dicts[pk] = new Dictionary<object, Dictionary<long, VersionEntry>>(SingletonPartitionedVersionTable.RECORD_CAPACITY);
            }

            Array.Resize(ref this.tableVisitors, expectedPartitionCount);
            for (int pk = prePartitionCount; pk < expectedPartitionCount; pk++)
            {
                this.tableVisitors[pk] = new SingletonPartitionedVersionTableVisitor(this.dicts[pk]);
            }

            Array.Resize(ref this.visitTicks, expectedPartitionCount);

            // Reshuffle Records
            HashSet<object> reshuffledKeys = new HashSet<object>();
            for (int pk = 0; pk < prePartitionCount; pk++)
            {
                List<object> toRemoveKeys = new List<object>();
                foreach (KeyValuePair<object, Dictionary<long, VersionEntry>> entry in this.dicts[pk])
                {
                    if (reshuffledKeys.Contains(entry.Key))
                    {
                        continue;
                    }
                    else
                    {
                        reshuffledKeys.Add(entry.Key);
                        int npk = this.VersionDb.PhysicalPartitionByKey(entry.Key);
                        if (pk == npk)
                        {
                            continue;
                        }
                        else
                        {
                            this.dicts[npk].Add(entry.Key, entry.Value);
                            toRemoveKeys.Add(entry.Key);
                        }
                    }
                }

                foreach (object key in toRemoveKeys)
                {
                    this.dicts[pk].Remove(key);
                }
                toRemoveKeys.Clear();
            }     
        }

        internal override void EnqueueVersionEntryRequest(VersionEntryRequest req, int execPartition = 0)
        {
            // Interlocked.Increment(ref VersionDb.EnqueuedRequests);

            // SingletonPartitionedVersionDb implementation 1
            //base.EnqueueVersionEntryRequest(req, execPartition);
            //while (!req.Finished) ;

            // SingletonPartitionedVersionDb implementation 2
            int pk = this.VersionDb.PhysicalPartitionByKey(req.RecordKey);
            long beginTicks = DateTime.Now.Ticks;
            this.tableVisitors[pk].Invoke(req);
            this.visitTicks[pk] += DateTime.Now.Ticks - beginTicks;

            // SingletonPartitionedVersionDb implementation 3
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
