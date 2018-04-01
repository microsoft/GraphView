namespace GraphView.Transaction
{
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


        // <summary>
        /// The bytes of -1 in long type, should mind that it must be a long type with 8 bytes
        /// It's used to compare txId or as a return value in lua script
        /// </summary>
        public static readonly byte[] NEGATIVE_ONE_BYTES = BitConverter.GetBytes(-1L);

        // <summary>
        /// The bytes of -2 in long type, should mind that it must be a long type with 8 bytes
        /// It's used to be a return value of some errors in lua
        /// </summary>
        public static readonly byte[] NEGATIVE_TWO_BYTES = BitConverter.GetBytes(-2L);

        // <summary>
        /// The bytes of -2 in long type, should mind that it must be a long type with 8 bytes
        /// It's used to be a return value of successful operations
        /// </summary
        public static readonly byte[] ZERO_BYTES = BitConverter.GetBytes(0L);

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

        private RedisLuaScriptManager RedisLuaManager
        {
            get
            {
                return RedisLuaScriptManager.Instance;
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

        /// <summary>
        /// Get redisDbIndex from meta hashset by tableId
        /// Take the System.Nullable<long> to wrap the return result, 
        ///     as it could return null if the table has not been found
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
    }

    internal partial class RedisVersionDb
    {
        internal override VersionTable CreateVersionTable(string tableId, long redisDbIndex)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.META_DB_INDEX);

                byte[] keyBytes = Encoding.ASCII.GetBytes(tableId);
                byte[] valueBytes = BitConverter.GetBytes(redisDbIndex);

                long result = redisClient.HSet(RedisVersionDb.META_TABLE_KEY, keyBytes, valueBytes);

                return this.GetVersionTable(tableId);
            }
        }

        internal override VersionTable GetVersionTable(string tableId)
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

        internal override bool DeleteTable(string tableId)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
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
                            this.versionTableMap.Remove(tableId);
                        }
                    }
                }

                return result == 1;
            }
        }
    }

    /// <summary>
    /// This part is the implementation of transaction store interfaces in redis version db
    /// TxTableEntry will be stored as a hashset in redis, and txId will be the hashId.
    /// We store in this format based on the fact we should always view and update status, 
    /// commit_time and commit_lower_bound. The hashset will be with better performance
    /// than parsing and unparsing binary data.
    /// 
    /// THING TO MIND: HSETNX in ServiceStack.Redis only supports a string as hashId
    /// To use this HSETNX, we should take txId.toString() as the hashId whereever we want
    /// to operate the TxTableEntry in redis.
    /// 
    /// IT'S IMPORTANT!!!!
    /// </summary>
    internal partial class RedisVersionDb
    {
        /// <summary>
        /// Get a unique transaction Id and store the txTableEntry into the redis
        /// This will be implemented in two steps:
        /// 1. try a random txId and ensure that it is unique in redis with the command HSETNX
        ///    If it is a unique id, set it in hset to occupy it with the same atomic operation
        /// 2. set other fields of txTableEntry 
        /// </summary>
        /// <returns>a transaction Id</returns>
        internal override long InsertNewTx()
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.TRANSACTION_DB_INDEX);

                long txId = 0, ret = 0;
                do
                {
                    txId = this.RandomLong();

                    string hashId = txId.ToString();
                    byte[] keyBytes = Encoding.ASCII.GetBytes("tx_id");
                    byte[] valueBytes = BitConverter.GetBytes(txId);

                    // If the hashId doesn't exist or field doesn't exist, return 1
                    // otherwise return 0
                    ret = redisClient.HSetNX(hashId, keyBytes, valueBytes);
                } while (ret == 0);

                TxTableEntry txTableEntry = new TxTableEntry(txId);

                byte[][] keysBytes =
                {
                    Encoding.ASCII.GetBytes("status"),
                    Encoding.ASCII.GetBytes("commit_time"),
                    Encoding.ASCII.GetBytes("commit_lower_bound")
                };

                byte[][] valuesBytes =
                {
                    BitConverter.GetBytes((int) txTableEntry.Status),
                    BitConverter.GetBytes(txTableEntry.CommitTime),
                    BitConverter.GetBytes(txTableEntry.CommitLowerBound)
                };

                redisClient.HMSet(txId.ToString(), keysBytes, valuesBytes);
                return txId;
            }
        }

        internal override TxTableEntry GetTxTableEntry(long txId)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.TRANSACTION_DB_INDEX);

                string hashId = txId.ToString();
                byte[][] keyBytes =
                {
                    Encoding.ASCII.GetBytes("status"),
                    Encoding.ASCII.GetBytes("commit_time"),
                    Encoding.ASCII.GetBytes("commit_lower_bound")
                };

                byte[][] valueBytes = redisClient.HMGet(hashId, keyBytes);
                if (valueBytes == null || valueBytes.Length == 0)
                {
                    return null;
                }

                return new TxTableEntry(
                    txId,
                    (TxStatus) BitConverter.ToInt32(valueBytes[0], 0),
                    BitConverter.ToInt64(valueBytes[1], 0),
                    BitConverter.ToInt64(valueBytes[2],0));
            }
        }

        internal override void UpdateTxStatus(long txId, TxStatus status)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.TRANSACTION_DB_INDEX);

                string hashId = txId.ToString();
                byte[] keyBytes = Encoding.ASCII.GetBytes("status");
                byte[] valueBytes = BitConverter.GetBytes((int) status);

                long ret = redisClient.HSet(hashId, keyBytes, valueBytes);
            }
        }

        internal override long GetAndSetCommitTime(long txId, long lowerBound)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.TRANSACTION_DB_INDEX);

                string hashId = txId.ToString();
                string sha1 = this.RedisLuaManager.GetLuaScriptSha1("GET_SET_COMMIT_TIME");
                byte[][] keys =
                {
                    Encoding.ASCII.GetBytes(hashId),
                    BitConverter.GetBytes(lowerBound),
                };

                try
                {
                    byte[][] returnBytes = redisClient.EvalSha(sha1, 1, keys);
                    if (returnBytes == null || returnBytes.Length == 0)
                    {
                        return -1;
                    }

                    return BitConverter.ToInt64(returnBytes[1], 0);
                }
                catch (RedisResponseException e)
                {
                    return -1;
                }
            }
        }

        internal override long UpdateCommitLowerBound(long txId, long commitTs)
        {
            using (RedisClient redisClient = (RedisClient)this.RedisManager.GetClient())
            {
                redisClient.ChangeDb(RedisVersionDb.TRANSACTION_DB_INDEX);

                string hashId = txId.ToString();
                string sha1 = this.RedisLuaManager.GetLuaScriptSha1("UPDATE_COMMIT_LOWER_BOUND");
                byte[][] keys =
                {
                    Encoding.ASCII.GetBytes(hashId),
                    BitConverter.GetBytes(commitTs),
                };

                try
                {
                    byte[][] returnBytes = redisClient.EvalSha(sha1, 1, keys);
                    if (returnBytes == null || returnBytes.Length == 0)
                    {
                        // TODO: throw exceptions
                    }

                    long ret = BitConverter.ToInt64(returnBytes[1], 0);
                    // There must be some errors happened inside the lua
                    if (ret == -2)
                    {
                        // TODO
                    }

                    return ret;
                }
                catch (RedisResponseException e)
                {
                    // TODO
                    return -1;
                }
            }
        }
    }
}
