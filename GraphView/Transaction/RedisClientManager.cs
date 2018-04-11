namespace GraphView.Transaction
{
    using ServiceStack.Redis;
    using ServiceStack.Redis.Generic;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// A singleton redis client manager to get redis client based on RedisManagerPool
    /// Can extract IRedisClient and IRedisNativeClient from this redis client manager 
    /// </summary>
    internal class RedisClientManager
    { 
        private static readonly object initLock = new object();
        private static IRedisClientsManager redisManagerPool;

        internal static IRedisClientsManager Instance
        {
            get
            {
                if (RedisClientManager.redisManagerPool == null)
                {
                    lock (RedisClientManager.initLock)
                    {
                        if (RedisClientManager.redisManagerPool == null)
                        {
                            RedisClientManagerConfig config = new RedisClientManagerConfig();
                            config.MaxReadPoolSize = 1000;
                            config.MaxWritePoolSize = 1000;
                            // TODO: read redis config from config files
                            string redisConnectionString = "127.0.0.1:6379";
                            RedisClientManager.redisManagerPool = 
                                new PooledRedisClientManager(
                                    new string[] { redisConnectionString },
                                    new string[] { redisConnectionString},
                                    config);
                        }
                    }
                }

                return RedisClientManager.redisManagerPool;
            }
        }

        internal RedisClient GetRedisClient()
        {
            IRedisClient redisClient = RedisClientManager.redisManagerPool.GetClient();
            return (RedisClient)redisClient;
        }
    }
}
