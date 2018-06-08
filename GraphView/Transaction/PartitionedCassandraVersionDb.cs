
namespace GraphView.Transaction
{
    using System.Threading;

    internal class PartitionedCassandraVersionDb : VersionDb
    {
        /// <summary>
        /// default keyspace
        /// </summary>
        public static readonly string DEFAULT_KEYSPACE = "versiondb";

        /// <summary>
        /// singleton instance
        /// </summary>
        private static volatile PartitionedCassandraVersionDb instance;

        /// <summary>
        /// lock to init the singleton instance
        /// </summary>
        private static readonly object initlock = new object();

        internal CassandraSessionManager SessionManager
        {
            get
            {
                return CassandraSessionManager.Instance;
            }
        }

        RequestQueue<TxEntryRequest>[] partitionedQueues;
        /// <summary>
        /// A visitor that translates tx entry requests to CQL queries, 
        /// sends them to Cassandra, and collects results and fill the request's result fields.
        /// 
        /// Since the visitor maintains no states across individual invokations, 
        /// only one instance suffice for all invoking threads. 
        /// </summary>
        PartitionedCassandraVersionDbVisitor cassandraVisitor;

        private PartitionedCassandraVersionDb(int partitionCount)
        {
            this.partitionedQueues = new RequestQueue<TxEntryRequest>[partitionCount];
            for (int pk = 0; pk < partitionCount; pk++)
            {
                this.partitionedQueues[pk] = new RequestQueue<TxEntryRequest>(partitionCount);
            }
            this.cassandraVisitor = new PartitionedCassandraVersionDbVisitor();

            for (int pk = 0; pk < partitionCount; pk++)
            {
                Thread thread = new Thread(this.Monitor);
                thread.Start(pk);
            }
        }

        internal static PartitionedCassandraVersionDb Instance(int partitionCount = 4)
        {
            if (PartitionedCassandraVersionDb.instance == null)
            {
                lock (initlock)
                {
                    if (PartitionedCassandraVersionDb.instance == null)
                    {
                        PartitionedCassandraVersionDb.instance = new PartitionedCassandraVersionDb(partitionCount);
                    }
                }
            }
            return PartitionedCassandraVersionDb.instance;
        }

        private void Monitor(object pk)
        {
            int partitionKey = (int)pk;
            while (true)
            {
                TxEntryRequest txReq = null;
                if (this.partitionedQueues[partitionKey].TryDequeue(out txReq))
                {
                    cassandraVisitor.Visit(txReq);
                } 
            }
        }

        internal override void EnqueueTxEntryRequest(long txId, TxEntryRequest txEntryRequest, int executorPK = 0)
        {
            int pk = this.PhysicalTxPartitionByKey(txId);

            partitionedQueues[pk].Enqueue(txEntryRequest, executorPK);

            while (!txEntryRequest.Finished)
            {
                lock(txEntryRequest)
                {
                    if (!txEntryRequest.Finished)
                    {
                        System.Threading.Monitor.Wait(txEntryRequest);
                    }
                }
            }
        }
    }
}
