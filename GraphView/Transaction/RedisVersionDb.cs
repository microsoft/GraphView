namespace GraphView.Transaction
{
    using RecordRuntime;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Newtonsoft.Json.Linq;
    using ServiceStack.Redis;
    using System.Threading;

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
        /// The generator of redis database index
        /// </summary>
        private readonly RedisDbIndexSequenceGenerator dbIndexGenerator;

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
        /// There are more reads and less write to versionTableMap in redisVersionDb
        /// 1. For most of operations, they need get redisVersionTable from versionTableMap and keep going
        /// 2. Only addVersionTable or deleteVersionTable will write the dictionary
        /// ReaderWriterLock is more suitable for the scene comparsed with ConCurrentDictionary
        /// Based on MSDN documents, ReaderWriterLockSlim is more effective than ReaderWriterLock
        /// </summary>
        private ReaderWriterLockSlim versionTableMapLock;

        /// <summary>
        /// Get RedisClient from the redis connection pool
        /// </summary>
        private RedisNativeClient RedisClient
        {
            get
            {
                return RedisClientManager.Instance.GetRedisClient();
            }
        }

        private RedisVersionDb() : this(RedisDbIndexSequenceGenerator.Instance)
        {
            
        }

        private RedisVersionDb(RedisDbIndexSequenceGenerator dbIndexGenerator)
        { 
            this.dbIndexGenerator = dbIndexGenerator;
            this.tableLock = new object();

            this.versionTableMap = new Dictionary<string, RedisVersionTable>();
            this.versionTableMapLock = new ReaderWriterLockSlim();
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

                // add write lock to ensure thread safe write
                versionTableMapLock.EnterWriteLock();
                try
                {
                    this.versionTableMap[tableId] = versionTable;
                }
                finally
                {
                    versionTableMapLock.ExitWriteLock();
                }
            }

            versionTableMapLock.EnterReadLock();
            try
            {
                return this.versionTableMap[tableId];
            }
            finally
            {
                versionTableMapLock.ExitReadLock();
            }
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
            this.RedisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

            byte[] keyBytes = Encoding.ASCII.GetBytes(tableId);
            byte[] valueBytes = this.RedisClient.HGet(RedisVersionDb.META_TABLE_KEY, keyBytes);

            if (valueBytes == null)
            {
                return null;
            }

            return BitConverter.ToInt64(valueBytes, 0);
        }

        protected bool AddVersionTable(string tableId)
        {
            this.RedisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

            long redisDbIndex = this.dbIndexGenerator.NextSequenceNumber();
            byte[] keyBytes = Encoding.ASCII.GetBytes(tableId);
            byte[] valueBytes = BitConverter.GetBytes(redisDbIndex);

            long result = this.RedisClient.HSet(RedisVersionDb.META_TABLE_KEY, keyBytes, valueBytes);

            return result == 1;
        }

        /// <summary>
        /// Delete version table from meta data
        /// </summary>
        /// <param name="tableId"></param>
        /// <returns></returns>
        protected bool DeleteVersionTable(string tableId)
        {
            this.RedisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

            byte[] keyBytes = Encoding.ASCII.GetBytes(tableId);

            long result = this.RedisClient.HDel(RedisVersionDb.META_TABLE_KEY, keyBytes);

            versionTableMapLock.EnterWriteLock();
            try
            {
                this.versionTableMap.RemoveKey(tableId);
            }
            finally
            {
                versionTableMapLock.ExitWriteLock();
            }
            // TODO: add lock and delete version table at the same time
            return result == 1;
        }

        /// <summary>
        /// Get all tableIds from meta data hashset
        /// </summary>
        /// <returns></returns>
        protected IList<string> GetAllVersionTables()
        {
            this.RedisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

            byte[][] keysBytes = this.RedisClient.HKeys(RedisVersionDb.META_TABLE_KEY);
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

    /// <summary>
    /// Override the part of basic version operations
    /// </summary>
    internal partial class RedisVersionDb
    {
        internal override VersionTable GetVersionTable(string tableId)
        {
            return base.GetVersionTable(tableId);
        }

        // Two thread-safe options should be ensured
        // 1. ensure the safty of versionTableMap's operations
        // 2. ensure that no more than a thread add versionTable to redis
        internal override bool InsertVersion(string tableId, object recordKey, object record, long txId,
            long readTimestamp)
        {
            RedisVersionTable versionTable = this.GetRedisVersionTable(tableId);
            // Create a new table if the table doesn't exists
            // use the lock to guarantee thread synchronization
            if (versionTable == null)
            {
                lock (this.tableLock)
                {
                    if (this.GetRedisVersionTable(tableId) == null)
                    {
                        this.AddVersionTable(tableId);
                    }
                }
                versionTable = this.GetRedisVersionTable(tableId);
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
