namespace GraphView.Transaction
{
    using RecordRuntime;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using ServiceStack.Redis;

    /// <summary>
    /// 1. Definition of fields of RedisVersionDb
    /// 2. Implementation of private methods of redis operation, all those operations are atomic operations
    /// </summary>
    internal partial class RedisVersionDb : VersionDb
    {
        /// <summary>
        /// The meta data of redisVersionDb will be stored in database META_DB_INDEX in Redis
        /// Map from tableId to dbIndex will be stored in a hashset data structure
        /// META_TABLE_KEY: the name of hashset in redis
        /// META_DB_INDEX : the defaule database index of redis
        /// </summary>
        private static readonly string META_TABLE_KEY = "meta:tables:hashset";
        private static readonly long META_DB_INDEX = 0;

        /// <summary>
        /// the map from tableId to redis version table
        /// </summary>
        private Dictionary<string, RedisVersionTable> versionTableMap;

        /// <summary>
        /// the singleton instance of RedisVersionDb
        /// </summary>
        private static volatile RedisVersionDb instance;

        /// <summary>
        /// the lock to guarantee the safety of table's creation and delete
        /// </summary>
        private readonly object tableLock;

        /// <summary>
        /// the lock to init the singleton instance
        /// </summary>
        private static readonly object initLock = new object();

        /// <summary>
        /// Get RedisClient from the redis connection pool
        /// </summary>
        private IRedisClientsManager RedisManager
        {
            get
            {
                return RedisClientManager.Instance;
            }
        }

        private RedisVersionDb()
        { 
            this.tableLock = new object();
            this.versionTableMap = new Dictionary<string, RedisVersionTable>();
        }

        internal static RedisVersionDb Instance
        {
            get
            {
                if (RedisVersionDb.Instance == null)
                {
                    lock (RedisVersionDb.initLock)
                    {
                        if (RedisVersionDb.Instance == null)
                        {
                            RedisVersionDb.instance = new RedisVersionDb();
                        }
                    }
                }
                return RedisVersionDb.instance;
            } 
        }

        public bool AddVersionTable(string tableId, long redisDbIndex)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                byte[] keyBytes = Encoding.ASCII.GetBytes(tableId);
                byte[] valueBytes = BitConverter.GetBytes(redisDbIndex);

                long result = redisClient.HSet(RedisVersionDb.META_TABLE_KEY, keyBytes, valueBytes);

                return result == 1;
            }
        }

        /// <summary>
        /// Delete version table from meta data table
        /// </summary>
        /// <param name="tableId"></param>
        /// <returns></returns>
        public bool DeleteVersionTable(string tableId)
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {

                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                byte[] keyBytes = Encoding.ASCII.GetBytes(tableId);
                long result = redisClient.HDel(RedisVersionDb.META_TABLE_KEY, keyBytes);

                if (this.versionTableMap.ContainsKey(tableId))
                {
                    lock (this.tableLock)
                    {
                        if (this.versionTableMap.ContainsKey(tableId))
                        {
                            this.versionTableMap.RemoveKey(tableId);
                        }
                    }
                }

                return result == 1;
            }
        }

        /// <summary>
        /// Get a versionTable instance by tableId from Dictionary or Redis
        /// </summary>
        /// <param name="tableId"></param>
        /// <returns></returns>
        protected RedisVersionTable GetRedisVersionTable(string tableId)
        {
            if (!this.versionTableMap.ContainsKey(tableId))
            { 
                long? redisDbIndex = this.GetTableRedisDbIndex(tableId);
                if (redisDbIndex == null)
                {
                    return null;
                }
                RedisVersionTable versionTable = new RedisVersionTable(tableId, redisDbIndex.Value);

                if (!this.versionTableMap.ContainsKey(tableId))
                {
                    lock (this.tableLock)
                    {
                        if (!this.versionTableMap.ContainsKey(tableId))
                        {
                            this.versionTableMap[tableId] = versionTable;
                        }
                    }
                }
                this.versionTableMap[tableId] = versionTable;
            }
            return this.versionTableMap[tableId];
        }

        /// <summary>
        /// Get redisDbIndex from meta hashset by tableId
        /// Take the System.Nullable<long> to wrap the return result, 
        ///     as it could return null if the table has not beed found
        /// </summary>
        /// <param name="tableId"></param>
        /// <returns></returns>
        protected long? GetTableRedisDbIndex(string tableId)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                byte[] keyBytes = Encoding.ASCII.GetBytes(tableId);
                byte[] valueBytes = redisClient.HGet(RedisVersionDb.META_TABLE_KEY, keyBytes);

                if (valueBytes == null)
                {
                    return null;
                }

                return BitConverter.ToInt64(valueBytes, 0);
            }
        }

        /// <summary>
        /// Get all tableIds from meta data hashset
        /// </summary>
        /// <returns></returns>
        protected IList<string> GetAllVersionTables()
        {
            using (RedisClient redisClient = (RedisClient) this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                byte[][] keysBytes = redisClient.HKeys(RedisVersionDb.META_TABLE_KEY);
                if (keysBytes == null)
                {
                    throw new ArgumentException("Invalid META_TABLE_KEY reference '{RedisVersionDb.META_TABLE_KEY}'");
                }

                List<string> tableIdList = new List<string>();
                foreach (byte[] keyBytes in keysBytes)
                {
                    string tableId = Encoding.ASCII.GetString(keyBytes);
                    tableIdList.Add(tableId);
                }

                return tableIdList;
            }
        }
    }

    /// <summary>
    /// Override the part of basic version operations
    /// </summary>
    internal partial class RedisVersionDb
    {
        internal override VersionTable GetVersionTable(string tableId)
        {
            return this.GetRedisVersionTable(tableId);
        }

        // End user must add the versionTable at first and then call the insertVersion method
        internal override bool InsertVersion(string tableId, object recordKey, object record, long txId,
            long readTimestamp)
        {
            RedisVersionTable versionTable = this.GetRedisVersionTable(tableId);
            if (versionTable == null)
            {
                return false;
            }
            return versionTable.InsertVersion(recordKey, record, txId, readTimestamp);
        }
    }

    /// <summary>
    /// Implementation of IDataStore
    /// </summary>
    internal partial class RedisVersionDb : IDataStore
    {
        public IList<Tuple<string, IndexSpecification>> GetIndexTables(string tableId)
        {
            throw new NotImplementedException();
        }

        public IList<string> GetTables()
        {
            return this.GetAllVersionTables();
        }
    }
}
