namespace GraphView.Transaction
{
    using ServiceStack.Redis;
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// A redis client manager for redis request. The redis manager hold a redis client pool 
    /// for every redis db. When a application client need a redis client, the client manager will 
    /// create or get the pool instance. And the real client will be fetched from redis client pool.
    /// 
    /// For every redis db, it has a daemon thread to send requests and collect results. 
    /// </summary>
    internal class RedisClientManager : IDisposable
    { 
        private static readonly int DEFAULT_REDIS_INSTANCE_COUNT = 1;

        /// <summary>
        /// the init lock to create singleton instance
        /// </summary>
        private static readonly object initLock = new object();

        /// <summary>
        /// The private variable to hold the instance
        /// </summary>
        private static RedisClientManager clientManager = null;

        /// <summary>
        /// The map from redisDbIndex to redisClientPool
        /// </summary>
        public static Dictionary<RedisConnectionKey, RedisConnectionPool> 
            clientPools = new Dictionary<RedisConnectionKey, RedisConnectionPool>();

        /// <summary>
        /// the lock for clientPool dictionary
        /// </summary>
        private static readonly object dictLock = new object();

        /// <summary>
        /// The redis connection strings of read and write
        /// </summary>
        internal string[] ReadWriteHosts { get; private set; }

        /// <summary>
        /// The number of redis instances
        /// </summary>
        internal int RedisInstanceCount { get; private set; }

        public static RedisClientManager Instance
        {
            get
            {
                if (RedisClientManager.clientManager == null)
                {
                    lock (RedisClientManager.initLock)
                    {
                        if (RedisClientManager.clientManager == null)
                        {
                            RedisClientManager.clientManager = new RedisClientManager();
                        }
                    }
                }
                return RedisClientManager.clientManager;
            }
        }

        private RedisClientManager()
        {
            this.RedisInstanceCount = RedisClientManager.DEFAULT_REDIS_INSTANCE_COUNT;
            this.ReadWriteHosts = new string[] { "127.0.0.1:6379"};
        }

        /// <summary>
        /// Config the redis instances by specifing an array of connection strings
        /// </summary>
        /// <param name="readWriteHosts">An array of connections strings, with host and port</param>
        internal void Config(string[] readWriteHosts)
        {
            if (readWriteHosts == null || readWriteHosts.Length == 0)
            {
                throw new ArgumentException("readWriteHosts must be not null or not empty");
            }

            this.RedisInstanceCount = readWriteHosts.Length;
            this.ReadWriteHosts = readWriteHosts;
        }
        
        internal RedisClient GetClient(long redisDbIndex, int partition)
        {
            RedisConnectionKey key = new RedisConnectionKey(partition, redisDbIndex);
            if (!RedisClientManager.clientPools.ContainsKey(key))
            {
                lock (RedisClientManager.dictLock)
                {
                    if (!RedisClientManager.clientPools.ContainsKey(key))
                    {
                        this.StartNewRedisClientPool(key);
                    }
                }
            }
            return RedisClientManager.clientPools[key].GetRedisClient();
        }

        internal RedisConnectionPool GetClientPool(long redisDbIndex, int partition)
        {
            RedisConnectionKey key = new RedisConnectionKey(partition, redisDbIndex);
            if (!RedisClientManager.clientPools.ContainsKey(key))
            {
                lock (RedisClientManager.dictLock)
                {
                    if (!RedisClientManager.clientPools.ContainsKey(key))
                    {
                        this.StartNewRedisClientPool(key);
                    }
                }
            }
            return RedisClientManager.clientPools[key];
        }

        public void Dispose()
        {
            foreach (RedisConnectionKey key in RedisClientManager.clientPools.Keys)
            {
                RedisClientManager.clientPools[key].Active = false;
            }
        }

        /// <summary>
        /// Start a new redis client pool, which will create an instance of RedisClientPool
        /// and start a new daemon thread to send requests and collect results
        /// </summary>
        /// <param name="redisDbIndex"></param>
        private void StartNewRedisClientPool(RedisConnectionKey key)
        {
            RedisConnectionPool pool = new RedisConnectionPool(this.ReadWriteHosts[key.RedisInstanceIndex], key.RedisDbIndex);
            RedisClientManager.clientPools.Add(key, pool);

            // If the redis version db in pipeline mode, start a daemon thread
            if (RedisVersionDb.Instance.PipelineMode)
            {
                Thread t = new Thread(new ThreadStart(pool.Monitor));
                t.Start();
            }
        }
    }
}
