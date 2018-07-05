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
		/// <summary>
		/// The lock for singleton instance
		/// </summary>
		private static readonly object initLock = new object();

		/// <summary>
		/// The private variable to hold the instance
		/// </summary>
		private static CassandraSessionManager sessionManager = null;

        // parameters
        internal int replication_factor = 1;
        internal ConsistencyLevel consistency_level = ConsistencyLevel.One;
        internal string[] contact_points = { "127.0.0.1" };

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

        public static CassandraSessionManager Instance2(string contactPoints, int replicationFactor, ConsistencyLevel consistencyLevel)
        {
            if (CassandraSessionManager.sessionManager == null)
            {
                lock (CassandraSessionManager.initLock)
                {
                    if (CassandraSessionManager.sessionManager == null)
                    {
                        CassandraSessionManager.sessionManager = new CassandraSessionManager(contactPoints, replicationFactor, consistencyLevel);
                    }
                }
            }
            return CassandraSessionManager.sessionManager;
        }

        private CassandraSessionManager()
		{
            // Ensure strong consistency
            // NOTE: IF there are more than 1 replica, `SetConsistencyLevel(ConsistencyLevel.Quorum)` 
            // to ensure strong consistency; otherwise,  `SetConsistencyLevel(ConsistencyLevel.One)` is enough.
            QueryOptions queryOptions = new QueryOptions().SetConsistencyLevel(this.consistency_level);//SetConsistencyLevel(ConsistencyLevel.Quorum);
                                                          //.SetSerialConsistencyLevel(ConsistencyLevel.Serial);
			this.cluster = Cluster.Builder().AddContactPoints(this.contact_points).WithQueryOptions(queryOptions).WithQueryTimeout(60000).Build();
			this.sessionPool = new Dictionary<string, ISession>();
        }

        private CassandraSessionManager(string contactPoints, int replicationFactor, ConsistencyLevel consistencyLevel)
        {
            this.contact_points = contactPoints.Split(',');
            this.replication_factor = replicationFactor;
            this.consistency_level = consistencyLevel;

            QueryOptions queryOptions = new QueryOptions().SetConsistencyLevel(this.consistency_level);
            this.cluster = Cluster.Builder().AddContactPoints(this.contact_points).WithQueryOptions(queryOptions).WithQueryTimeout(60000).Build();
            this.sessionPool = new Dictionary<string, ISession>();
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
                                                  "{'class': 'SimpleStrategy', 'replication_factor': " + this.replication_factor + " };");
						this.sessionPool[keyspace] = this.cluster.Connect(keyspace);
					}
				}
			}

			return this.sessionPool[keyspace];
		}
    }
}
