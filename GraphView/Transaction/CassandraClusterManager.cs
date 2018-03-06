namespace GraphView.Transaction
{
    using Cassandra;

    /// <summary>
    /// A cluster manager for cassandra connection, the current implementation is
    /// a singleton cluster instance with a specify host address
    /// </summary>
    class CassandraClusterManager
    {
        /// <summary>
        /// The lock for singleton instance
        /// </summary>
        private static readonly object initLock = new object();

        /// <summary>
        /// the cluster instance of cassandra
        /// </summary>
        private static Cluster cluster;

        internal static Cluster CassandraCluster
        {
            get
            {
                if (CassandraClusterManager.cluster == null)
                {
                    lock (CassandraClusterManager.initLock)
                    {
                        if (CassandraClusterManager.cluster == null)
                        {
                            string host = "127.0.0.1";
                            CassandraClusterManager.cluster = 
                                Cluster.Builder().AddContactPoints(host).Build();
                        }
                    }
                }
                return CassandraClusterManager.cluster;
            }
        }
    }
}
