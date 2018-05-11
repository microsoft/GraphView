
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

        /// <summary>
        /// The version table maps
        /// </summary>
        private readonly Dictionary<string, SingletonPartitionedVersionTable> versionTables;

        /// <summary>
        /// The transaction table map, txId => txTableEntry
        /// </summary>
        private readonly Dictionary<long, TxTableEntry>[] txTable;

        /// <summary>
        /// requests of all tx operations
        /// </summary>
        private readonly Queue<TxEntryRequest>[] txEntryRequestQueues;
        private readonly SpinLock[] queueLocks;
        private readonly PartitionTxEntryRequestVisitor[] txRequestVisitors;


        /// <summary>
        /// The count of partitions, thus the number of dicts for a version table
        /// </summary>
        internal int PartitionCount { get; private set; }

        private SingletonPartitionedVersionDb(int partitionCount)
        {
            this.versionTables = new Dictionary<string, SingletonPartitionedVersionTable>();

            this.txTable = new Dictionary<long, TxTableEntry>[partitionCount];
            this.txEntryRequestQueues = new Queue<TxEntryRequest>[partitionCount];
            this.queueLocks = new SpinLock[partitionCount];
            this.txRequestVisitors = new PartitionTxEntryRequestVisitor[partitionCount];

            for (int pid = 0; pid < partitionCount; pid++)
            {
                this.txTable[pid] = new Dictionary<long, TxTableEntry>(100000);
                this.txEntryRequestQueues[pid] = new Queue<TxEntryRequest>();
                this.queueLocks[pid] = new SpinLock();
                this.txRequestVisitors[pid] = new PartitionTxEntryRequestVisitor(this.txTable[pid]);
            }

            this.PartitionCount = partitionCount;
            this.PhysicalPartitionByKey = key => key.GetHashCode() % this.PartitionCount;
        }

        /// <summary>
        /// The method to get the version db's singleton instance
        /// </summary>
        /// <param name="partitionCount">The number of partitions</param>
        /// <returns></returns>
        internal static SingletonPartitionedVersionDb Instance(int partitionCount = 4)
        {
            if (SingletonPartitionedVersionDb.instance == null)
            {
                lock (initlock)
                {
                    if (SingletonPartitionedVersionDb.instance == null)
                    {
                        SingletonPartitionedVersionDb.instance = new SingletonPartitionedVersionDb(partitionCount);
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

            for (int pid = 0; pid < this.PartitionCount; pid ++)
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

        /// <summary>
        /// Enqueue Transcation Entry Requests
        /// </summary>
        /// <param name="txId">The specify txId to partition</param>
        /// <param name="txEntryRequest">The given request</param>
        private void EnqueueTxEntryRequest(long txId, TxEntryRequest txEntryRequest)
        {
            int pk = this.PhysicalPartitionByKey(txId);
            bool lockTaken = false;
            try
            {
                this.queueLocks[pk].Enter(ref lockTaken);
                this.txEntryRequestQueues[pk].Enqueue(txEntryRequest);
            }
            finally
            {
                if (lockTaken)
                {
                    this.queueLocks[pk].Exit();
                }
            }
        }

        private IEnumerable<TxEntryRequest> DequeueTxEntryRequest(int partitionKey)
        {
            TxEntryRequest[] reqArray = null;
            Queue<TxEntryRequest> queue = this.txEntryRequestQueues[partitionKey];

            if (queue.Count > 0)
            {
                bool lockTaken = false;
                try
                {
                    this.queueLocks[partitionKey].Enter(ref lockTaken);
                    if (queue.Count > 0)
                    {
                        reqArray = queue.ToArray();
                        queue.Clear();
                    }
                }
                finally
                {
                    if (lockTaken)
                    {
                        this.queueLocks[partitionKey].Exit();
                    }
                }
            }

            return reqArray;
        }

        internal override void Visit(string tableId, int partitionKey)
        {
            // Here try to flush the tx requests
            if (tableId == VersionDb.TX_TABLE)
            {
                IEnumerable<TxEntryRequest> reqArray = this.DequeueTxEntryRequest(partitionKey);
                if (reqArray == null)
                {
                    return;
                }

                foreach (TxEntryRequest req in reqArray)
                {
                    this.txRequestVisitors[partitionKey].Invoke(req);
                }
            }
            else
            {
                SingletonPartitionedVersionTable versionTable = 
                    this.GetVersionTable(tableId) as SingletonPartitionedVersionTable;
                if (versionTable != null)
                {
                    versionTable.Visit(partitionKey);
                }
            }
        }

        internal override GetTxEntryRequest EnqueueGetTxEntry(long txId)
        {
            GetTxEntryRequest req = new GetTxEntryRequest(txId);
            this.EnqueueTxEntryRequest(txId, req);
            return req;
        }

        internal override InsertTxIdRequest EnqueueInsertTxId(long txId)
        {
            InsertTxIdRequest req = new InsertTxIdRequest(txId);
            this.EnqueueTxEntryRequest(txId, req);
            return req;
        }

        internal override NewTxIdRequest EnqueueNewTxId()
        {
            long txId = StaticRandom.RandIdentity();
            NewTxIdRequest req = new NewTxIdRequest(txId);
            this.EnqueueTxEntryRequest(txId, req);
            return req;
        }

        internal override RecycleTxRequest EnqueueRecycleTx(long txId)
        {
            RecycleTxRequest req = new RecycleTxRequest(txId);
            this.EnqueueTxEntryRequest(txId, req);
            return req;
        }

        internal override SetCommitTsRequest EnqueueSetCommitTs(long txId, long proposedCommitTs)
        {
            SetCommitTsRequest req = new SetCommitTsRequest(txId, proposedCommitTs);
            this.EnqueueTxEntryRequest(txId, req);
            return req;
        }

        internal override UpdateCommitLowerBoundRequest EnqueueUpdateCommitLowerBound(long txId, long lowerBound)
        {
            UpdateCommitLowerBoundRequest lowerBoundReq = new UpdateCommitLowerBoundRequest(txId, lowerBound);
            this.EnqueueTxEntryRequest(txId, lowerBoundReq);
            return lowerBoundReq;
        }

        internal override UpdateTxStatusRequest EnqueueUpdateTxStatus(long txId, TxStatus status)
        {
            UpdateTxStatusRequest req = new UpdateTxStatusRequest(txId, status);
            this.EnqueueTxEntryRequest(txId, req);
            return req;
        }
    }
}
