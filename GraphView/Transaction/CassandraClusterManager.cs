namespace GraphView.Transaction
{
    using Cassandra;
	using System;
	using System.Collections.Generic;

	/// <summary>
	/// A session manager for cassandra connection, the current implementation is
	/// a singleton cluster instance with a specify host address
	/// </summary>
	class CassandraSessionManager
    {
		private static readonly int DEFAULT_CLUSTER_NODE_COUNT = 1;

		/// <summary>
		/// The lock for singleton instance
		/// </summary>
		private static readonly object initLock = new object();

		/// <summary>
		/// The private variable to hold the instance
		/// </summary>
		private static CassandraSessionManager sessionManager = null;

		/// <summary>
		/// the cluster instance of cassandra
		/// </summary>
		private Cluster cluster = null;

		/// <summary>
		/// A map from keyspace to session.
		/// </summary>
		private Dictionary<string, ISession> sessionPool;

		/// <summary>
		/// the lock for sessionPool dictionary
		/// </summary>
		private readonly object dictLock = new object();

		/// <summary>
		/// The cassandra connection strings of read and write
		/// </summary>
		internal string[] ReadWriteHosts { get; private set; }

		/// <summary>
		/// The number of cluster hosts
		/// </summary>
		internal int ClusterNodeCount { get; private set; }

        public static CassandraSessionManager Instance
        {
            get
            {
                if (CassandraSessionManager.sessionManager == null)
                {
                    lock (CassandraSessionManager.initLock)
                    {
                        if (CassandraSessionManager.sessionManager == null)
                        {
							CassandraSessionManager.sessionManager = new CassandraSessionManager();
                        }
                    }
                }
				return CassandraSessionManager.sessionManager;
            }
        }

		private CassandraSessionManager()
		{
			this.ClusterNodeCount = CassandraSessionManager.DEFAULT_CLUSTER_NODE_COUNT;
			this.ReadWriteHosts = new string[] { "127.0.0.1" };

            // Ensure strong consistency
            // NOTE: IF there are more than 1 replica, `SetConsistencyLevel(ConsistencyLevel.Quorum)` 
            // to ensure strong consistency; otherwise,  `SetConsistencyLevel(ConsistencyLevel.One)` is enough.
            QueryOptions queryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One)
                                                          .SetSerialConsistencyLevel(ConsistencyLevel.Serial);
			this.cluster = Cluster.Builder().AddContactPoints(this.ReadWriteHosts).WithQueryOptions(queryOptions).Build();
			this.sessionPool = new Dictionary<string, ISession>();
		}

		internal ISession GetSession(string keyspace, int threadId = -1)
		{
            //string sessionKey = keyspace + "_" + threadId.ToString();
			if (!this.sessionPool.ContainsKey(keyspace))
			{
				lock (this.dictLock)
				{
					if (!this.sessionPool.ContainsKey(keyspace))
					{
                        ISession session = cluster.Connect();
                        session.Execute($@"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = " +
                                   "{'class':'SimpleStrategy', 'replication_factor':'1'};");    // there is just 1 replica
						this.sessionPool[keyspace] = this.cluster.Connect(keyspace);
					}
				}
			}

			return this.sessionPool[keyspace];
		}
	}
}
