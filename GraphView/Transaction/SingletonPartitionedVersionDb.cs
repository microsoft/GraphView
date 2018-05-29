
namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class SingletonPartitionedVersionDb : VersionDb
    {
        private static volatile SingletonPartitionedVersionDb instance;

        private static readonly object initlock = new object();

        internal static int EnqueuedRequests = 0;

        /// <summary>
        /// Whether the version db is in deamon mode
        /// In deamon mode, for every partition, it will have a thread to flush
        /// In non-daemon mode, the executor will flush requests
        /// </summary>
        internal bool DaemonMode { get; set; } = true;

        internal bool Active { get; set; } = false;

        internal long FlushWaitTicks { get; set; } = 0L;

        internal TxResourceManager[] resourceManagers;
        /// <summary>
        /// The version table maps
        /// </summary>

        /// <summary>
        /// The transaction table map, txId => txTableEntry
        /// </summary>
        private readonly Dictionary<long, TxTableEntry>[] txTable;

        private SingletonPartitionedVersionDb(int partitionCount, bool daemonMode)
            : base(partitionCount)
        {
            this.txTable = new Dictionary<long, TxTableEntry>[partitionCount];

            for (int pid = 0; pid < partitionCount; pid++)
            {
                this.txTable[pid] = new Dictionary<long, TxTableEntry>(100000);
                this.dbVisitors[pid] = new PartitionedVersionDbVisitor(this.txTable[pid]);
            }

            this.resourceManagers = new TxResourceManager[partitionCount];
            for (int i = 0; i < partitionCount; i++)
            {
                this.resourceManagers[i] = new TxResourceManager();
            }

            this.PhysicalPartitionByKey = key => Math.Abs(key.GetHashCode()) % this.PartitionCount;
            this.PhysicalTxPartitionByKey = key => (int)((long)key / TxRange.range);

            this.DaemonMode = daemonMode;
            if (this.DaemonMode)
            {
                this.Active = true;
                for (int pk = 0; pk < this.PartitionCount; pk++)
                {
                    Thread thread = new Thread(this.Monitor);
                    thread.Start(pk);
                }
            }
        }

        /// <summary>
        /// The method to get the version db's singleton instance
        /// </summary>
        /// <param name="partitionCount">The number of partitions</param>
        /// <returns></returns>
        internal static SingletonPartitionedVersionDb Instance(int partitionCount = 4, bool daemonMode = false)
        {
            if (SingletonPartitionedVersionDb.instance == null)
            {
                lock (initlock)
                {
                    if (SingletonPartitionedVersionDb.instance == null)
                    {
                        SingletonPartitionedVersionDb.instance = new SingletonPartitionedVersionDb(partitionCount, daemonMode);
                    }
                }
            }
            return SingletonPartitionedVersionDb.instance;
        }

        internal override void Clear()
        {
            // Clear contents of the version table at first
            foreach (string key in this.versionTables.Keys)
            {
                this.versionTables[key].Clear();
            }
            this.versionTables.Clear();

            for (int pid = 0; pid < this.PartitionCount; pid++)
            {
                if (this.txTable[pid] != null)
                {
                    this.txTable[pid].Clear();
                }
            }
        }

        internal override VersionTable CreateVersionTable(string tableId, long redisDbIndex = 0)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                lock (this.versionTables)
                {
                    if (!this.versionTables.ContainsKey(tableId))
                    {
                        SingletonPartitionedVersionTable vtable = new SingletonPartitionedVersionTable(this, tableId, this.PartitionCount);
                        this.versionTables.Add(tableId, vtable);
                    }
                }
            }

            return this.versionTables[tableId];
        }

        internal override bool DeleteTable(string tableId)
        {
            if (this.versionTables.ContainsKey(tableId))
            {
                lock (this.versionTables)
                {
                    if (this.versionTables.ContainsKey(tableId))
                    {
                        this.versionTables.Remove(tableId);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        internal override IEnumerable<string> GetAllTables()
        {
            return this.versionTables.Keys;
        }

        internal override VersionTable GetVersionTable(string tableId)
        {
            if (!this.versionTables.ContainsKey(tableId))
            {
                return null;
            }
            return this.versionTables[tableId];
        }

        internal override TxResourceManager GetResourceManagerByPartitionIndex(int partition)
        {
            if (partition >= this.PartitionCount)
            {
                throw new ArgumentException("partition should be smaller then partitionCount");
            }
            return this.resourceManagers[partition];
        }

        internal void Monitor(object obj)
        {
            int pk = (int)obj;
            long lastFlushTicks = DateTime.Now.Ticks;

            while (this.Active)
            {
                while (DateTime.Now.Ticks - lastFlushTicks < this.FlushWaitTicks) { }
                lastFlushTicks = DateTime.Now.Ticks;
                this.Visit(VersionDb.TX_TABLE, pk);
                foreach (string tableId in this.versionTables.Keys.ToArray())
                {
                    this.Visit(tableId, pk);
                }
            }
        }

        internal override void EnqueueTxEntryRequest(long txId, TxEntryRequest txEntryRequest, int execPartition = 0)
        {
            int pk = this.PhysicalTxPartitionByKey(txId);
            // Interlocked.Increment(ref SingletonPartitionedVersionDb.EnqueuedRequests);
            if (pk == execPartition)
            {
                this.dbVisitors[pk].Invoke(txEntryRequest);
            }
            else
            {
                base.EnqueueTxEntryRequest(txId, txEntryRequest, execPartition);
            }
        }
    }
}
