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

        private Dictionary<int, ISession> cacheSessions;

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

        internal string[] contactPoints;
        //internal int replicationFactor = 3;
        internal int replicationFactor = 1;

        internal static string CQL_CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS {0} WITH replication = " +
            "{'class': 'SimpleStrategy', 'replication_factor': {1} };";

        /// <summary>
        /// Count how many CQLs are executed
        /// </summary>
        public static int CqlCnt = 0;
        public static int CqlIfCnt = 0;
        public static void CqlCountShow()
        {
            Console.WriteLine("CQL total  = {0}", CassandraSessionManager.CqlCnt + CassandraSessionManager.CqlIfCnt);
            Console.WriteLine("CQL        = {0}", CassandraSessionManager.CqlCnt);
            Console.WriteLine("CQL IF     = {0}", CassandraSessionManager.CqlIfCnt);
        }

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

        public static CassandraSessionManager Instance2(string[] contactPoints, int replicationFactor)
        {
            if (CassandraSessionManager.sessionManager == null)
            {
                lock (CassandraSessionManager.initLock)
                {
                    if (CassandraSessionManager.sessionManager == null)
                    {
                        CassandraSessionManager.sessionManager = new CassandraSessionManager(contactPoints, replicationFactor);
                    }
                }
            }
            return CassandraSessionManager.sessionManager;
        }

        private CassandraSessionManager()
		{
			this.ClusterNodeCount = CassandraSessionManager.DEFAULT_CLUSTER_NODE_COUNT;
            //this.contactPoints = new string[] { "127.0.0.1" };
            this.contactPoints = new string[] { "10.6.0.4", "10.6.0.5", "10.6.0.6", "10.6.0.12", "10.6.0.13", "10.6.0.14", "10.6.0.15", "10.6.0.16", "10.6.0.17", "10.6.0.18" };
            //this.contactPoints = new string[] { "10.6.0.4" };

            // Ensure strong consistency
            // NOTE: IF there are more than 1 replica, `SetConsistencyLevel(ConsistencyLevel.Quorum)` 
            // to ensure strong consistency; otherwise,  `SetConsistencyLevel(ConsistencyLevel.One)` is enough.
            QueryOptions queryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.Quorum);//SetConsistencyLevel(ConsistencyLevel.Quorum);
                                                          //.SetSerialConsistencyLevel(ConsistencyLevel.Serial);
			this.cluster = Cluster.Builder().AddContactPoints(this.contactPoints).WithQueryOptions(queryOptions).WithQueryTimeout(60000).Build();
			this.sessionPool = new Dictionary<string, ISession>();
            this.cacheSessions = new Dictionary<int, ISession>();
        }

        private CassandraSessionManager(string[] contactPoints, int replicationFactor)
        {
            //this.ClusterNodeCount = CassandraSessionManager.DEFAULT_CLUSTER_NODE_COUNT;
            //this.replicationFactor = replicationFactor;

            //// Ensure strong consistency
            //// NOTE: IF there are more than 1 replica, `SetConsistencyLevel(ConsistencyLevel.Quorum)` 
            //// to ensure strong consistency; otherwise,  `SetConsistencyLevel(ConsistencyLevel.One)` is enough.
            //QueryOptions queryOptions = new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One)
            //                                              .SetSerialConsistencyLevel(ConsistencyLevel.Serial);
            //this.cluster = Cluster.Builder().AddContactPoints(this.contactPoints).WithQueryOptions(queryOptions).Build();
            //this.sessionPool = new Dictionary<string, ISession>();
        }

        internal ISession GetSession(string keyspace)
		{
			if (!this.sessionPool.ContainsKey(keyspace))
			{
				lock (this.dictLock)
				{
					if (!this.sessionPool.ContainsKey(keyspace))
					{
                        cluster.Connect().Execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = " +
                                                  "{'class': 'SimpleStrategy', 'replication_factor': 3};");
						this.sessionPool[keyspace] = this.cluster.Connect(keyspace);
					}
				}
			}

			return this.sessionPool[keyspace];
		}

        internal ISession GetSession(int threadId, string keyspace)
        {
            if (!this.cacheSessions.ContainsKey(threadId))
            {
                lock (this.dictLock)
                {
                    if (!this.cacheSessions.ContainsKey(threadId))
                    {
                        cluster.Connect().Execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = " +
                                                  "{'class': 'SimpleStrategy', 'replication_factor': 3};");
                        this.cacheSessions[threadId] = this.cluster.Connect(keyspace);
                    }
                }
            }

            return this.cacheSessions[threadId];
        }
    }
}
