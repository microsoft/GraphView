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
        /// <summary>
        /// The map from redisDbIndex to redisClientPool
        /// </summary>
        private Dictionary<RedisConnectionKey, RedisConnectionPool> 
            clientPools = new Dictionary<RedisConnectionKey, RedisConnectionPool>();

        /// <summary>
        /// the lock for clientPool dictionary
        /// </summary>
        private readonly object dictLock = new object();

        /// <summary>
        /// The redis connection strings of read and write
        /// </summary>
        private string[] readWriteHosts;

        /// <summary>
        /// A redis lua manager instance from the redis version db
        /// ONLY FOR REQUEST VISITOR
        /// </summary>
        private RedisLuaScriptManager redisLuaScriptManager;

        /// <summary>
        /// The number of redis instances
        /// </summary>
        internal int RedisInstanceCount
        {
            get
            {
                return this.readWriteHosts.Length;
            }
        }

        internal RedisClientManager()
        {
            this.readWriteHosts = new string[] { "127.0.0.1:6379"};
        }

        /// <summary>
        /// Init a redis client manager with given read and write hosts
        /// </summary>
        /// <param name="readWriteHosts">An array of connections strings, with host and port</param>
        internal RedisClientManager(string[] readWriteHosts, RedisLuaScriptManager luaScriptManager) 
        {
            if (readWriteHosts == null || readWriteHosts.Length == 0)
            {
                throw new ArgumentException("readWriteHosts at least have a host");
            }
            this.readWriteHosts = readWriteHosts;
            this.redisLuaScriptManager = luaScriptManager;
        }

        internal RedisClientManager(string[] readWriteHosts) : this(readWriteHosts, null)
        {

        }
        
        internal RedisClient GetClient(long redisDbIndex, int partition)
        {
            RedisConnectionKey key = new RedisConnectionKey(partition, redisDbIndex);
            if (!this.clientPools.ContainsKey(key))
            {
                lock (this.dictLock)
                {
                    if (!this.clientPools.ContainsKey(key))
                    {
                        this.StartNewRedisClientPool(key);
                    }
                }
            }
            return this.clientPools[key].GetRedisClient();
        }

        /// <summary>
        /// Only for debug, the reason why we need such a method in YCSBBenchmarkTest/GetCurrentCommandCount
        /// </summary>
        /// <param name="redisDbIndex"></param>
        /// <param name="partition"></param>
        /// <returns></returns>
        internal RedisClient GetLastestClient(long redisDbIndex, int partition)
        {
            RedisConnectionPool pool = new RedisConnectionPool(this.readWriteHosts[partition], redisDbIndex, this.redisLuaScriptManager);
            return pool.GetRedisClient();
        }

        internal RedisConnectionPool GetClientPool(long redisDbIndex, int partition)
        {
            RedisConnectionKey key = new RedisConnectionKey(partition, redisDbIndex);
            if (!this.clientPools.ContainsKey(key))
            {
                lock (this.dictLock)
                {
                    if (!this.clientPools.ContainsKey(key))
                    {
                        this.StartNewRedisClientPool(key);
                    }
                }
            }
            return this.clientPools[key];
        }

        public void Dispose()
        {
            foreach (RedisConnectionKey key in this.clientPools.Keys)
            {
                this.clientPools[key].Dispose();
            }
        }

        /// <summary>
        /// Start a new redis client pool, which will create an instance of RedisClientPool
        /// and start a new daemon thread to send requests and collect results
        /// </summary>
        /// <param name="redisDbIndex"></param>
        private void StartNewRedisClientPool(RedisConnectionKey key)
        {
            RedisConnectionPool pool = new RedisConnectionPool(
                this.readWriteHosts[key.RedisInstanceIndex], key.RedisDbIndex, this.redisLuaScriptManager);
            this.clientPools.Add(key, pool);

            // If the redis version db in pipeline mode, start a daemon thread
            // if (key.RedisDbIndex != 0)
            // {
            //     Thread t = new Thread(new ThreadStart(pool.Monitor));
            //     t.Start();
            // }
        }
    }
}
