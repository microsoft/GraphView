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
        private static volatile RedisClientManager instance;
        private static readonly object initLock = new object();
        private readonly RedisManagerPool redisManagerPool;

        private RedisClientManager()
        {
            // TODO: read redis config from config files
            string redisConnectionString = "";
            this.redisManagerPool = new RedisManagerPool(redisConnectionString);
        }

        internal static RedisClientManager Instance
        {
            get
            {
                if (RedisClientManager.instance == null)
                {
                    lock (RedisClientManager.initLock)
                    {
                        if (RedisClientManager.instance == null)
                        {
                            RedisClientManager.instance = new RedisClientManager();
                        }
                    }
                }

                return RedisClientManager.instance;
            }
        }

        internal RedisClient GetRedisClient()
        {
            IRedisClient redisClient = RedisClientManager.Instance.redisManagerPool.GetClient();
            return (RedisClient)redisClient;
        }
    }
}
