using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GraphView.Transaction
{
    /// <summary>
    /// A redis client manager for redis request. The redis manager hold a redis client pool 
    /// for every redis db. When a application client need a redis client, the client manager will 
    /// create or get the pool instance. And the real client will be fetched from redis client pool.
    /// 
    /// For every redis db, it has a daemon thread to send requests and collect results. 
    /// </summary>
    class RedisClientManager : IDisposable
    {
        private static readonly string DEFAULT_HOST = "127.0.0.1";

        private static readonly int DEFAULT_PORT = 6379;

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
        public static Dictionary<long, RedisClientPool> clientPools = new Dictionary<long, RedisClientPool>();

        /// <summary>
        /// the lock for clientPool dictionary
        /// </summary>
        private static readonly object dictLock = new object();

        /// <summary>
        /// The host of redis
        /// </summary>
        internal string Host { get; set; }

        /// <summary>
        /// The port of redis
        /// </summary>
        internal int Port { get; set; }

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

        public RedisClientManager()
        {
            this.Host = RedisClientManager.DEFAULT_HOST;
            this.Port = RedisClientManager.DEFAULT_PORT;
        }
        
        internal RedisClient GetClient(long redisDbIndex)
        {
            if (!RedisClientManager.clientPools.ContainsKey(redisDbIndex))
            {
                lock (RedisClientManager.dictLock)
                {
                    if (!RedisClientManager.clientPools.ContainsKey(redisDbIndex))
                    {
                        this.StartNewRedisClientPool(redisDbIndex);
                    }
                }
            }
            return RedisClientManager.clientPools[redisDbIndex].GetRedisClient();
        }

        internal RedisClientPool GetClientPool(long redisDbIndex)
        {
            if (!RedisClientManager.clientPools.ContainsKey(redisDbIndex))
            {
                lock (RedisClientManager.dictLock)
                {
                    if (!RedisClientManager.clientPools.ContainsKey(redisDbIndex))
                    {
                        this.StartNewRedisClientPool(redisDbIndex);
                    }
                }
            }
            return RedisClientManager.clientPools[redisDbIndex];
        }

        public void Dispose()
        {
            foreach (long redisDbIndex in RedisClientManager.clientPools.Keys)
            {
                RedisClientManager.clientPools[redisDbIndex].Active = false;
            }
        }

        /// <summary>
        /// Start a new redis client pool, which will create an instance of RedisClientPool
        /// and start a new daemon thread to send requests and collect results
        /// </summary>
        /// <param name="redisDbIndex"></param>
        private void StartNewRedisClientPool(long redisDbIndex)
        {
            RedisClientPool pool = new RedisClientPool(this.Host, this.Port, redisDbIndex);
            RedisClientManager.clientPools.Add(redisDbIndex, pool);

            // If the redis version db in pipeline mode, start a daemon thread
            if (RedisVersionDb.Instance.PipelineMode)
            {
                Thread t = new Thread(new ThreadStart(pool.Monitor));
                t.Start();
            }
        }
    }
}
