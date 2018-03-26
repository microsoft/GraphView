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
        /// The defalut hashset key for the meta table
        /// meta table is a map of tableId to redisDbIndex
        /// </summary>          
        public static readonly string META_TABLE_KEY = "meta:tables:hashset";

        /// <summary>
        /// The defalut hashset key for the lua script command
        /// meta_script is a map from script_name to sha1
        /// </summary>
        public static readonly string META_SCRIPT_KEY = "meta:scripts:hashset";

        /// <summary>
        /// The default meta database index
        /// </summary>
        public static readonly long META_DB_INDEX = 0;

        /// <summary>
        /// The default transaction database index
        /// </summary>
        public static readonly long TRANSACTION_DB_INDEX = 1;

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

            this.checkAndRegisterScripts();
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

        /// <summary>
        /// For every possible lua scripts command, check if it has been in the redis cache.
        /// If it has not been loaded into the cache, then register and load it
        /// </summary>
        protected void checkAndRegisterScripts()
        {
            // implement the cas for hset command
            // 0: comparsion succeeds and updated an existed version
            // 1: comparsion succeeds and inserted a new version
            // 2: comparsion fails
            // 3: other errors, which means the command runs error
            /*
                 -- eval 'lua_code' 1 hashkey field oldValue newValue
                 local ver = redis.call('HGET', KEYS[1], ARGV[1]);
                 if not ver or ver == ARGV[2] then
                     return redis.call('HSET', KEYS[1], ARGV[1], ARGV[3]);
                 end
                 return 2
                */
            string HSetCAS = @"local ver = redis.call('HGET', KEYS[1], ARGV[1]); if not ver or ver == ARGV[2]
                then return redis.call('HSET', KEYS[1], ARGV[1], ARGV[3]); end return 2";
            this.RegisterLuaScripts("HSET_CAS", HSetCAS);

            // Other lua commands
        }

        /// <summary>
        /// Register common lua scripts to the cache avoidoing update scripts 
        /// every time, which will reduce the bandwidth
        /// </summary>
        /// <returns>true or false</returns>
        private bool RegisterLuaScripts(string scriptKey, string luaBody)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                // Extract the command sha1
                byte[] scriptKeyBytes = Encoding.UTF8.GetBytes(scriptKey);
                byte[] shaBytes = redisClient.HGet(RedisVersionDb.META_SCRIPT_KEY, scriptKeyBytes);

                // We will register the lua script only if the sha1 isn't in script table or sha1 is not in the redis cache 
                bool hasRegistered = false;
                if (shaBytes != null)
                {
                    byte[][] returnBytes = redisClient.ScriptExists(new byte[][] { shaBytes });
                    if (returnBytes != null)
                    {
                        // The return value == 1 means scripts have been registered
                        hasRegistered = BitConverter.ToInt64(returnBytes[0], 0) == 1;
                    }
                }

                // Register the lua scripts when it has not been registered
                if (!hasRegistered)
                {
                    // register and load the script
                    byte[] scriptSha1Bytes = redisClient.ScriptLoad(luaBody);
                    if (scriptSha1Bytes == null)
                    {
                        return false;
                    }
                    // insert into the script hashset
                    long result = redisClient.HSet(RedisVersionDb.META_SCRIPT_KEY, scriptKeyBytes, scriptSha1Bytes);
                    return result == 1;
                }

                return true;
            }
        }
    }

    internal partial class RedisVersionDb
    {
        
    }
}
