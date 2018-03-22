namespace GraphView.Transaction
{
    using System;
    using System.Collections.Generic;
    using ServiceStack.Redis;
    using RecordRuntime;
    using Newtonsoft.Json.Linq;
    using System.Text;

    internal partial class RedisVersionTable : VersionTable
    {
        /// <summary>
        /// The redis database index of the current table
        /// </summary>
        private readonly long redisDbIndex;

        /// <summary>
        /// Get redisClient from the client pool
        /// </summary>
        private IRedisClientsManager RedisManager
        {
            get
            {
                return RedisClientManager.Instance;
            }
        }

        public RedisVersionTable(string tableId, long redisDbIndex)
            : base(tableId)
        {
            this.redisDbIndex = redisDbIndex;
        }
    }
}
